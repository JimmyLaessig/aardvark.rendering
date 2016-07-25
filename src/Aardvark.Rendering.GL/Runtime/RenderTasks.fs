﻿namespace Aardvark.Rendering.GL

#nowarn "9"

open System
open System.Linq
open System.Diagnostics
open System.Threading
open System.Collections.Generic
open Aardvark.Base
open Aardvark.Base.Rendering
open Aardvark.Base.Runtime
open Aardvark.Base.Incremental
open OpenTK.Graphics.OpenGL4
open Aardvark.Rendering.GL.Compiler
open System.Runtime.CompilerServices
open Microsoft.FSharp.NativeInterop

module RenderTasks =
    open System.Collections.Generic


    [<AbstractClass>]
    type AbstractRenderTask(manager : ResourceManager, fboSignature : IFramebufferSignature, config : IMod<BackendConfiguration>, shareTextures : bool, shareBuffers : bool) =
        inherit AdaptiveObject()
        let ctx = manager.Context
        let renderTaskLock = RenderTaskLock()
        let manager = ResourceManager(manager.Context, Some (fboSignature, renderTaskLock), shareTextures, shareBuffers)
        let allBuffers = manager.DrawBufferManager.CreateConfig(fboSignature.ColorAttachments |> Map.toSeq |> Seq.map (snd >> fst) |> Set.ofSeq)
        let structureChanged = Mod.custom ignore


        let mutable isDisposed = false
        let currentContext = Mod.init Unchecked.defaultof<ContextHandle>


        let scope =
            { 
                currentContext = currentContext
                stats = ref FrameStatistics.Zero
                drawBuffers = NativePtr.toNativeInt allBuffers.Buffers
                drawBufferCount = allBuffers.Count 
                usedTextureSlots = ref RefSet.empty
                usedUniformBufferSlots = ref RefSet.empty
                structuralChange = structureChanged
            }

        let mutable frameId = 0UL
//        let drawBuffers = 
//            fboSignature.ColorAttachments 
//                |> Map.toList 
//                |> List.map (fun (i,_) -> int DrawBuffersEnum.ColorAttachment0 + i |> unbox<DrawBuffersEnum>)
//                |> List.toArray
        
        let beforeRender = new System.Reactive.Subjects.Subject<unit>()
        let afterRender = new System.Reactive.Subjects.Subject<unit>()

        member x.CurrentContext = currentContext
        member x.BeforeRender = beforeRender
        member x.AfterRender = afterRender

        member x.StructureChanged() =
            transact (fun () -> structureChanged.MarkOutdated())

        member internal x.pushDebugOutput() =
            let wasEnabled = GL.IsEnabled EnableCap.DebugOutput
            let c = config.GetValue x
            if c.useDebugOutput then
                if frameId = 0UL then
                    Log.warn "debug output enabled"
                match ContextHandle.Current with
                    | Some v -> v.AttachDebugOutputIfNeeded()
                    | None -> Report.Warn("No active context handle in RenderTask.Run")
                GL.Enable EnableCap.DebugOutput

            wasEnabled

        member internal x.popDebugOutput(wasEnabled : bool) =
            let c = config.GetValue x
            if wasEnabled <> c.useDebugOutput then
                if wasEnabled then GL.Enable EnableCap.DebugOutput
                else GL.Disable EnableCap.DebugOutput

        member internal x.pushFbo (desc : OutputDescription) =
            let fbo = desc.framebuffer |> unbox<Framebuffer>
            let old = Array.create 4 0
            let mutable oldFbo = 0
            OpenTK.Graphics.OpenGL.GL.GetInteger(OpenTK.Graphics.OpenGL.GetPName.Viewport, old)
            OpenTK.Graphics.OpenGL.GL.GetInteger(OpenTK.Graphics.OpenGL.GetPName.FramebufferBinding, &oldFbo)

            let handle = fbo.Handle |> unbox<int> 

            if ExecutionContext.framebuffersSupported then
                GL.BindFramebuffer(OpenTK.Graphics.OpenGL4.FramebufferTarget.Framebuffer, handle)
                GL.Check "could not bind framebuffer"
        


                GL.DepthMask(true)
                GL.StencilMask(0xFFFFFFFFu)
                GL.Enable(EnableCap.DepthClamp)
                
                for (index,(sem,_)) in fbo.Signature.ColorAttachments |> Map.toSeq do
                    match Map.tryFind sem desc.colorWrite with
                        | Some v -> 
                            GL.ColorMask(
                                index, 
                                (v &&& ColorWriteMask.Red)   <> ColorWriteMask.None, 
                                (v &&& ColorWriteMask.Green) <> ColorWriteMask.None,
                                (v &&& ColorWriteMask.Blue)  <> ColorWriteMask.None, 
                                (v &&& ColorWriteMask.Alpha) <> ColorWriteMask.None
                            )
                        | None -> 
                            GL.ColorMask(index, true, true, true, true)

                for (index, sem) in fbo.Signature.Images |> Map.toSeq do
                    match Map.tryFind sem desc.images with
                        | Some img ->
                            let tex = img.texture |> unbox<Texture>
                            GL.BindImageTexture(index, tex.Handle, img.level, false, img.slice, TextureAccess.ReadWrite, unbox (int tex.Format))
                        | None -> 
                            GL.ActiveTexture(int TextureUnit.Texture0 + index |> unbox)
                            GL.BindTexture(TextureTarget.Texture2D, 0)

            elif handle <> 0 then
                failwithf "cannot render to texture on this OpenGL driver"

            GL.Viewport(desc.viewport.Min.X, desc.viewport.Min.Y, desc.viewport.SizeX, desc.viewport.SizeY)
            GL.Check "could not set viewport"

       

            oldFbo, old

        member internal x.popFbo (desc : OutputDescription, (oldFbo : int, old : int[])) =
            if ExecutionContext.framebuffersSupported then
                GL.BindFramebuffer(OpenTK.Graphics.OpenGL4.FramebufferTarget.Framebuffer, oldFbo)
                GL.Check "could not bind framebuffer"

                for (index, sem) in desc.framebuffer.Signature.Images |> Map.toSeq do
                    GL.ActiveTexture(int TextureUnit.Texture0 + index |> unbox)
                    GL.BindTexture(TextureTarget.Texture2D, 0)


            GL.Viewport(old.[0], old.[1], old.[2], old.[3])
            GL.Check "could not set viewport"

        member internal x.incrementFrameId() =
            frameId <- frameId + 1UL
            

        abstract member Update : Framebuffer -> FrameStatistics
        abstract member Perform : Framebuffer -> FrameStatistics
        abstract member Release : unit -> unit

        member x.Dispose() =
            if not isDisposed then
                isDisposed <- true
                let dummy = ref 0
                currentContext.Outputs.Consume(dummy) |> ignore
                x.Release()

        member x.Config = config
        member x.Context = ctx
        member x.FramebufferSignature = fboSignature
        member x.Scope = scope
        member x.RenderTaskLock = renderTaskLock
        member x.ResourceManager = manager

        member x.Run(caller : IAdaptiveObject, desc : OutputDescription) =
            let fbo = desc.framebuffer // TODO: fix outputdesc
            if not <| fboSignature.IsAssignableFrom fbo.Signature then
                failwithf "incompatible FramebufferSignature\nexpected: %A but got: %A" fboSignature fbo.Signature

            x.EvaluateAlways caller (fun () ->
                x.OutOfDate <- true

                use token = ctx.ResourceLock 
                if currentContext.UnsafeCache <> ctx.CurrentContextHandle.Value then
                    transact (fun () -> Mod.change currentContext ctx.CurrentContextHandle.Value)

                let fbo =
                    match fbo with
                        | :? Framebuffer as fbo -> fbo
                        | _ -> failwithf "unsupported framebuffer: %A" fbo


                let debugState = x.pushDebugOutput()
                

                let innerStats = 
                    renderTaskLock.Run (fun () -> 
                        let ru = x.Update fbo

                        let fboState = x.pushFbo desc
                        beforeRender.OnNext()
                        let rp = x.Perform fbo
                        afterRender.OnNext()
                        x.popFbo (desc, fboState)

                        ru + rp
                    )

                x.popDebugOutput debugState

                

                GL.BindVertexArray 0
                GL.BindBuffer(BufferTarget.DrawIndirectBuffer,0)
            

                frameId <- frameId + 1UL
                innerStats + !scope.stats
            )

        interface IDisposable with
            member x.Dispose() = x.Dispose()

        interface IRenderTask with
            member x.Run(a : IAdaptiveObject, b : OutputDescription) = RenderingResult(b.framebuffer, x.Run(a,b))
            member x.FrameId = frameId
            member x.FramebufferSignature = fboSignature
            member x.Runtime = Some ctx.Runtime
            

    [<AbstractClass>]
    type AbstractSubTask(parent : AbstractRenderTask) =

        let programUpdateWatch  = Stopwatch()
        let sortWatch           = Stopwatch()
        let runWatch            = OpenGlStopwatch()

        member x.ProgramUpdate (f : unit -> 'a) =
            programUpdateWatch.Restart()
            let res = f()
            programUpdateWatch.Stop()
            res

        member x.Sorting (f : unit -> 'a) =
            sortWatch.Restart()
            let res = f()
            sortWatch.Stop()
            res

        member x.Execution (f : unit -> 'a) =
            runWatch.Restart()
            let res = f()
            runWatch.Stop()
            res

        member x.Parent = parent

        abstract member Perform : Framebuffer -> FrameStatistics
        abstract member Dispose : unit -> unit
        abstract member Add : PreparedMultiRenderObject -> unit
        abstract member Remove : PreparedMultiRenderObject -> unit


        member x.Run(fbo : Framebuffer) =
            let plain = x.Perform(fbo)
            lazy (
                { plain with
                    RenderPassCount = 1.0
                    SortingTime = MicroTime sortWatch.Elapsed
                    ProgramUpdateTime = MicroTime programUpdateWatch.Elapsed
                    ExecutionTime = runWatch.ElapsedGPU
                    SubmissionTime = runWatch.ElapsedCPU
                }
            )

        interface IDisposable with
            member x.Dispose() = x.Dispose()

    type SortKey = list<int>

    type ProjectionComparer(projections : list<RenderObject -> IMod>) =

        let rec getRenderObject (ro : IRenderObject) =
            match ro with
                | :? RenderObject as ro -> ro
                | :? MultiRenderObject as ro -> ro.Children |> List.head |> getRenderObject
                | :? PreparedRenderObject as ro -> ro.Original
                | :? PreparedMultiRenderObject as ro -> ro.First.Original
                | _ -> failwithf "[ProjectionComparer] unknown RenderObject: %A" ro

        let ids = ConditionalWeakTable<IMod, ref<int>>()
        let mutable currentId = 0
        let getId (m : IMod) =
            match ids.TryGetValue m with
                | (true, r) -> !r
                | _ ->
                    let id = Interlocked.Increment &currentId
                    ids.Add(m, ref id)
                    id

        let maxKey = Int32.MaxValue :: (projections |> List.map (fun _ -> Int32.MaxValue))

        let keys = ConditionalWeakTable<IRenderObject, SortKey>()
        let project (ro : IRenderObject) =
            let ro = getRenderObject ro

            match keys.TryGetValue ro with
                | (true, key) -> key
                | _ ->
                    if ro.Id < 0 then
                        maxKey
                    else
                        let key = projections |> List.map (fun p -> p ro |> getId)
                        keys.Add(ro, key)
                        key


        interface IComparer<IRenderObject> with
            member x.Compare(l : IRenderObject, r : IRenderObject) =
                let left = project l
                let right = project r
                compare left right

    type StaticOrderSubTask(parent : AbstractRenderTask) =
        inherit AbstractSubTask(parent)
        static let empty = new PreparedMultiRenderObject([PreparedRenderObject.empty])
        let objects = CSet.ofList [empty]

        let mutable hasProgram = false
        let mutable currentConfig = BackendConfiguration.Default
        let mutable program : IRenderProgram = Unchecked.defaultof<_>
        let structuralChange = Mod.custom ignore
        let scope = { parent.Scope with structuralChange = structuralChange }

        // TODO: add AdaptiveProgram creator not taking a separate key but simply comparing the values
        let objectsWithKeys = objects |> ASet.map (fun o -> (o :> IRenderObject, o))

        let reinit (self : StaticOrderSubTask) (config : BackendConfiguration) =
            // if the config changed or we never compiled a program
            // we need to do something
            if config <> currentConfig || not hasProgram then

                // if we have a program we'll dispose it now
                if hasProgram then program.Dispose()

                // use the config to create a comparer for IRenderObjects
                let comparer =
                    match config.sorting with
                        | RenderObjectSorting.Grouping projections -> 
                            ProjectionComparer(projections) :> IComparer<_>

                        | RenderObjectSorting.Static comparer -> 
                            { new IComparer<_> with 
                                member x.Compare(l, r) =
                                    if l.Id = r.Id then 0
                                    elif l.Id < 0 then -1
                                    elif r.Id < 0 then 1
                                    else comparer.Compare(l,r)
                            }

                        | Arbitrary ->
                            { new IComparer<_> with 
                                member x.Compare(l, r) =
                                    if l.Id < 0 then -1
                                    elif r.Id < 0 then 1
                                    else 0
                            }

                        | RenderObjectSorting.Dynamic create ->
                            failwith "[AbstractRenderTask] dynamic sorting not implemented"

                // create the new program
                let newProgram = 
                    match config.execution, config.redundancy with
                        | ExecutionEngine.Interpreter, _ ->
                            Log.line "using interpreted program"
                            RenderProgram.Interpreter.runtime scope objects

                        | ExecutionEngine.Native, RedundancyRemoval.Static -> 
                            Log.line "using optimized native program"
                            RenderProgram.Native.optimized scope comparer objectsWithKeys

                        | ExecutionEngine.Native, RedundancyRemoval.None -> 
                            Log.line "using unoptimized native program"
                            RenderProgram.Native.unoptimized scope comparer objectsWithKeys

                        | ExecutionEngine.Managed, RedundancyRemoval.Static -> 
                            Log.line "using optimized managed program"
                            RenderProgram.Managed.optimized scope comparer objectsWithKeys

                        | ExecutionEngine.Managed, RedundancyRemoval.None -> 
                            Log.line "using unoptimized managed program"
                            RenderProgram.Managed.unoptimized scope comparer objectsWithKeys

                        | ExecutionEngine.Debug, RedundancyRemoval.Static -> 
                            Log.line "using optimized debug program"
                            RenderProgram.Debug.optimized scope comparer objectsWithKeys

                        | ExecutionEngine.Debug, RedundancyRemoval.None -> 
                            Log.line "using unoptimized debug program"
                            RenderProgram.Debug.unoptimized scope comparer objectsWithKeys


                        | ExecutionEngine.Unmanaged, RedundancyRemoval.Static -> 
                            Log.line "using optimized unmanaged program"
                            RenderProgram.GLVM.optimized scope comparer objectsWithKeys

                        | ExecutionEngine.Unmanaged, RedundancyRemoval.Runtime -> 
                            Log.line "using runtime-optimized unmanaged program"
                            RenderProgram.GLVM.runtime scope comparer objectsWithKeys

                        | ExecutionEngine.Unmanaged, RedundancyRemoval.None -> 
                            Log.line "using unoptimized unmanaged program"
                            RenderProgram.GLVM.unoptimized scope comparer objectsWithKeys

                        | t ->
                            failwithf "[GL] unsupported backend configuration: %A" t


                // finally we store the current config/ program and set hasProgram to true
                program <- newProgram
                hasProgram <- true
                currentConfig <- config

        override x.Perform(fbo) =
            let config = parent.Config.GetValue parent
            reinit x config

            let updateStats = 
                x.ProgramUpdate (fun () -> program.Update parent)

            let stats = 
                x.Execution (fun () -> program.Run(fbo))

            stats
               

        override x.Dispose() =
            if hasProgram then
                hasProgram <- false
                program.Dispose()
                objects.Clear()
        
        override x.Add(o) = 
            transact (fun () -> 
                structuralChange.MarkOutdated()
                objects.Add o |> ignore
            )

        override x.Remove(o) = 
            transact (fun () -> 
                structuralChange.MarkOutdated()
                objects.Remove o |> ignore
            )


    

                
    [<AllowNullLiteral>]
    type AdaptiveGLVMFragment(obj : PreparedMultiRenderObject, adaptiveCode : IAdaptiveCode<Instruction>) =
        inherit AdaptiveObject()

        let boundingBox : IMod<Box3d> =
            if obj.First.Id < 0 then Mod.constant Box3d.Invalid
            else
                match Ag.tryGetAttributeValue obj.First.Original.AttributeScope "GlobalBoundingBox" with
                    | Success box -> box
                    | _ -> failwith "[GL] could not get BoundingBox for RenderObject"
        let mutable currentBox = Box3d.Invalid

        let mutable prev : AdaptiveGLVMFragment = null
        let mutable next : AdaptiveGLVMFragment = null

        let code = List.toArray adaptiveCode.Content
        let frag = GLVM.vmCreate()
        let blocksWithContent = code |> Array.map (fun content -> (GLVM.vmNewBlock frag, content))

        let blockTable =
            code 
                |> Array.mapi (fun i m ->
                    if m.IsConstant then 
                        None
                    else
                        Some (m :> IAdaptiveObject, blocksWithContent.[i])
                   )
                |> Array.choose id
                |> Dictionary.ofArray

        let getArgs (o : Instruction) =
            o.Arguments |> Array.map (fun arg ->
                match arg with
                    | :? int as i -> nativeint i
                    | :? int64 as i -> nativeint i
                    | :? nativeint as i -> i
                    | :? float32 as f -> BitConverter.ToInt32(BitConverter.GetBytes(f), 0) |> nativeint
                    | :? PtrArgument as p ->
                        match p with
                            | Ptr32 p -> p
                            | Ptr64 p -> p
                    | _ -> failwith "invalid argument"
            )

        let writeBlock (id : int) (instructions : seq<Instruction>) =
            GLVM.vmClearBlock(frag, id)
            for i in instructions do
                match getArgs i with
                    | [| a |] -> GLVM.vmAppend1(frag, id, i.Operation, a)
                    | [| a; b |] -> GLVM.vmAppend2(frag, id, i.Operation, a, b)
                    | [| a; b; c |] -> GLVM.vmAppend3(frag, id, i.Operation, a, b, c)
                    | [| a; b; c; d |] -> GLVM.vmAppend4(frag, id, i.Operation, a, b, c, d)
                    | [| a; b; c; d; e |] -> GLVM.vmAppend5(frag, id, i.Operation, a, b, c, d, e)
                    | _ -> failwithf "invalid instruction: %A" i

        let dirtyBlocks = HashSet blocksWithContent
        
        override x.InputChanged (transaction : obj, o : IAdaptiveObject) =
            match blockTable.TryGetValue o with
                | (true, dirty) -> lock dirtyBlocks (fun () -> dirtyBlocks.Add dirty |> ignore)
                | _ -> ()

        member x.Object = obj

        member x.BoundingBox = currentBox

        member x.Update(caller : IAdaptiveObject) =
            x.EvaluateIfNeeded caller () (fun () ->
                let blocks = 
                    lock dirtyBlocks (fun () ->
                        let all = Seq.toList dirtyBlocks
                        dirtyBlocks.Clear()
                        all
                    )

                for (block, content) in blocks do
                    let c = content.GetValue x
                    writeBlock block c

                currentBox <- boundingBox.GetValue x

            )

        member x.Handle = frag

        member x.Next
            with get() = next
            and set v = 
                next <- v
                if isNull v then GLVM.vmLink(frag, 0n)
                else GLVM.vmLink(frag, v.Handle)

        member x.Prev
            with get() = prev
            and set v = prev <- v

        member x.Dispose() =
            adaptiveCode.Dispose()
            if not (isNull prev) then prev.Next <- next
            if not (isNull next) then next.Prev <- prev
            GLVM.vmDelete frag

        interface IDisposable with
            member x.Dispose() = x.Dispose()

    type SortedGLVMProgram(parent : CameraSortedSubTask, objects : aset<PreparedMultiRenderObject>, createComparer : Ag.Scope -> IMod<IComparer<PreparedMultiRenderObject>>) =
        inherit AbstractRenderProgram<AdaptiveGLVMFragment>()
        static do GLVM.vmInit()
        static let empty = new PreparedMultiRenderObject([PreparedRenderObject.empty])
        let fragments = objects |> ASet.mapUse (fun o -> new AdaptiveGLVMFragment(o, RenderProgram.Compiler.compileFull { parent.Scope with stats = ref FrameStatistics.Zero } o))
        let fragmentReader = fragments.GetReader()
        let mutable vmStats = VMStats()
        let last = new AdaptiveGLVMFragment(empty, RenderProgram.Compiler.compileFull parent.Scope empty)
        let mutable first : AdaptiveGLVMFragment = last

        let mutable comparer = None

        let mutable disposeCnt = 0

        let getComparer (f : seq<AdaptiveGLVMFragment>) =
            match comparer with
                | Some cmp -> cmp
                | None ->
                    if Seq.isEmpty f then
                        Mod.constant { new IComparer<_> with member x.Compare(a,b) = 0 }
                    else
                        let fst = Seq.head f
                        let c = createComparer fst.Object.Original.AttributeScope
                        comparer <- Some c
                        c


        member private x.sort (f : seq<AdaptiveGLVMFragment>) : list<AdaptiveGLVMFragment> =
            let comparer = getComparer f
            let cmp = comparer.GetValue x
            f |> Seq.sortWith (fun a b -> cmp.Compare(a.Object, b.Object)) |> Seq.toList

        override x.Update(dirty : HashSet<_>) =
            let deltas = fragmentReader.GetDelta()
            for d in deltas do
                match d with
                    | Add f -> dirty.Add f |> ignore
                    | Rem f -> dirty.Remove f |> ignore

            for d in dirty do d.Update x

            parent.Sorting (fun () ->
                let ordered = x.sort fragmentReader.Content

                let mutable current = null
                for f in ordered do
                    f.Prev <- current
                    if isNull current then first <- f
                    else current.Next <- f
                    current <- f

                if not <| isNull current then current.Next <- last
                else first <- last
            )

        override x.Run(fbo) =
            
            if disposeCnt > 0 then
                failwithf "Running disposed glvmprogram"

            vmStats.TotalInstructions <- 0
            vmStats.RemovedInstructions <- 0
            if not (isNull first) then
                last.Next <- null
                GLVM.vmRun(first.Handle, VMMode.RuntimeRedundancyChecks, &vmStats)

            { FrameStatistics.Zero with
                InstructionCount = float vmStats.TotalInstructions
                ActiveInstructionCount = float (vmStats.TotalInstructions-vmStats.RemovedInstructions)
            }

        override x.Dispose() =
            if Interlocked.Increment &disposeCnt = 1 then
                last.Dispose()
                fragmentReader.Dispose()    
            else
                Log.warn "double dispose"

    and SortedInterpreterProgram(parent : CameraSortedSubTask, objects : aset<PreparedMultiRenderObject>, createComparer : Ag.Scope -> IMod<IComparer<PreparedMultiRenderObject>>) =
        inherit AbstractRenderProgram()

        let reader = objects.GetReader()
        let mutable arr = null

        let mutable comparer = None
        let mutable activeInstructions = 0
        let mutable totalInstructions = 0

        let getComparer (f : seq<PreparedMultiRenderObject>) =
            match comparer with
                | Some cmp -> cmp
                | None ->
                    if Seq.isEmpty f then
                        Mod.constant { new IComparer<_> with member x.Compare(a,b) = 0 }
                    else
                        let fst = Seq.head f
                        let c = createComparer fst.Original.AttributeScope
                        comparer <- Some c
                        c


        override x.Update() =
            reader.Update(x)

            parent.Sorting (fun () ->
                let comparer = getComparer reader.Content
                let cmp = comparer.GetValue x
                arr <- reader.Content |> Seq.sortWith (fun a b -> cmp.Compare(a,b)) |> Seq.toArray
            )

        override x.Run(fbo) =
            Interpreter.run fbo (fun gl ->
                for a in arr do gl.render a

                { FrameStatistics.Zero with
                    ActiveInstructionCount = float gl.EffectiveInstructions
                    InstructionCount = float gl.TotalInstructions
                }
            )

        override x.Dispose() =
            reader.Dispose()

    and CameraSortedSubTask(order : RenderPassOrder, parent : AbstractRenderTask) =
        inherit AbstractSubTask(parent)
        do GLVM.vmInit()

        let structuralChange = Mod.custom ignore
        let scope = { parent.Scope with structuralChange = structuralChange }

        let mutable hasCameraView = false
        let mutable cameraView = Mod.constant Trafo3d.Identity
        
        let objects = CSet.empty
        let boundingBoxes = Dictionary<PreparedMultiRenderObject, IMod<Box3d>>()

        let bb (o : PreparedMultiRenderObject) =
            boundingBoxes.[o].GetValue(parent)

        let mutable program = Unchecked.defaultof<IRenderProgram>
        let mutable hasProgram = false
        let mutable currentConfig = BackendConfiguration.Debug

        let createComparer (scope : Ag.Scope) =
            Mod.custom (fun self ->
                let cam = cameraView.GetValue self
                let pos = cam.GetViewPosition()

                match order with
                    | RenderPassOrder.BackToFront ->
                        { new IComparer<PreparedMultiRenderObject> with
                            member x.Compare(l,r) = compare ((bb r).GetMinimalDistanceTo pos) ((bb l).GetMinimalDistanceTo pos)
                        }
                    | _ ->
                        { new IComparer<PreparedMultiRenderObject> with
                            member x.Compare(l,r) = compare ((bb l).GetMinimalDistanceTo pos) ((bb r).GetMinimalDistanceTo pos)
                        }
            )


        let reinit (self : CameraSortedSubTask) (c : BackendConfiguration) =
            if currentConfig <> c || not hasProgram then
                if hasProgram then
                    program.Dispose()

                let newProgram = 
                    match c.execution with
                        | ExecutionEngine.Interpreter -> new SortedInterpreterProgram(self, objects, createComparer) :> IRenderProgram
                        | _ -> new SortedGLVMProgram(self, objects, createComparer) :> IRenderProgram

                program <- newProgram
                hasProgram <- true
                currentConfig <- c

        member x.Scope = scope

        override x.Perform(fbo) =
            let cfg = parent.Config.GetValue parent
            reinit x cfg

            let updateStats = 
                x.ProgramUpdate (fun () -> program.Update parent)

            let stats = 
                x.Execution (fun () -> program.Run(fbo))

            stats



        override x.Dispose() =
            if hasProgram then
                program.Dispose()
                hasProgram <- false

            objects.Clear()
            hasCameraView <- false
            cameraView <- Mod.constant Trafo3d.Identity
        
        override x.Add(o) = 
            if not hasCameraView then
                let o = o.First.Original

                match o.Uniforms.TryGetUniform (o.AttributeScope, Symbol.Create "ViewTrafo") with
                    | Some (:? IMod<Trafo3d> as view) -> 
                        hasCameraView <- true
                        cameraView <- view
                    | _ -> ()

            if o.First.Id < 0 then
                 boundingBoxes.[o] <- Mod.constant Box3d.Invalid
            else
                match Ag.tryGetAttributeValue o.Original.AttributeScope "GlobalBoundingBox" with
                    | Success b -> boundingBoxes.[o] <- b
                    | _ -> failwithf "[GL] could not get bounding-box for RenderObject"

            transact (fun () -> 
                structuralChange.MarkOutdated()
                objects.Add o |> ignore
            )

        override x.Remove(o) =
            boundingBoxes.Remove o |> ignore
            transact (fun () -> 
                structuralChange.MarkOutdated()
                objects.Remove o |> ignore
            )
                

    type RenderObjectStats() =
        inherit DirtyTrackingAdaptiveObject<IMod<DrawCallStats>>()

        let oldValues = Dict<IMod<DrawCallStats>, DrawCallStats>()
        let mutable drawCalls = 0
        let mutable effectiveCalls = 0
        let mutable resourceCount = 0

        let mutable added = 0
        let mutable removed = 0

        let add (stats : DrawCallStats) =
            match stats with
                | NoDraw -> ()
                | DirectDraw(c, i) ->
                    // TODO: assuming that DrawIndirects have only 1 instance
                    Interlocked.Add(&drawCalls, c) |> ignore
                    Interlocked.Add(&effectiveCalls, i) |> ignore
                | IndirectDraw c ->
                    Interlocked.Add(&drawCalls, 1) |> ignore
                    Interlocked.Add(&effectiveCalls, c) |> ignore
                    
        let remove (stats : DrawCallStats) =
            match stats with
                | NoDraw -> ()
                | DirectDraw(c, i) ->
                    // TODO: assuming that DrawIndirects have only 1 instance
                    Interlocked.Add(&drawCalls, -c) |> ignore
                    Interlocked.Add(&effectiveCalls, -i) |> ignore
                | IndirectDraw c ->
                    Interlocked.Add(&drawCalls, -1) |> ignore
                    Interlocked.Add(&effectiveCalls, -c) |> ignore

        member x.Add(o : PreparedRenderObject) =
            added <- added + 1
            resourceCount <- resourceCount + o.ResourceCount
            let value = o.DrawCallStats.GetValue x
            oldValues.[o.DrawCallStats] <- value
            add value

        member x.Remove(o : PreparedRenderObject) =
            removed <- removed + 1
            resourceCount <- resourceCount - o.ResourceCount
            match oldValues.TryRemove o.DrawCallStats with
                | (true, old) -> 
                    lock o.DrawCallStats (fun () -> o.DrawCallStats.Outputs.Remove x |> ignore)
                    remove old
                | _ -> ()

        member x.GetValue(caller : IAdaptiveObject) =
            x.EvaluateAlways' caller (fun dirty ->
                if x.OutOfDate then
                    for d in dirty do
                        let value = d.GetValue x
                        match oldValues.TryGetValue d with
                            | (true, old) -> 
                                remove old
                                add value
                            | _ ->
                                () // removed

                        oldValues.[d] <- value
                                
                let add = Interlocked.Exchange(&added, 0) 
                let rem = Interlocked.Exchange(&removed, 0)
                { FrameStatistics.Zero with 
                    DrawCallCount = float drawCalls
                    EffectiveDrawCallCount = float effectiveCalls 
                    VirtualResourceCount = float resourceCount
                    AddedRenderObjects = float add
                    RemovedRenderObjects = float rem
                }
            )

        interface IMod with
            member x.IsConstant = false
            member x.GetValue caller = x.GetValue caller :> obj

        interface IMod<FrameStatistics> with
            member x.GetValue caller = x.GetValue caller

    type RenderTask(man : ResourceManager, fboSignature : IFramebufferSignature, objects : aset<IRenderObject>, config : IMod<BackendConfiguration>, shareTextures : bool, shareBuffers : bool) as this =
        inherit AbstractRenderTask(man, fboSignature, config, shareTextures, shareBuffers)
        
        let ctx = man.Context
        let resources = new Aardvark.Base.Rendering.ResourceInputSet()
        let inputSet = InputSet(this) 
        let resourceUpdateWatch = OpenGlStopwatch()
        let callStats = RenderObjectStats()
        let structuralChange = Mod.init ()
        
        let primitivesGenerated = OpenGlQuery(QueryTarget.PrimitivesGenerated)

        let add (ro : PreparedRenderObject) = 
            let all = ro.Resources |> Seq.toList
            for r in all do resources.Add r

            callStats.Add ro

            let old = ro.Activation
            ro.Activation <- 
                { new IDisposable with
                    member x.Dispose() =
                        old.Dispose()
                        for r in all do resources.Remove r
                        callStats.Remove ro
                }
            ro

        let rec prepareRenderObject (ro : IRenderObject) =
            let res = this.ResourceManager.Prepare(fboSignature, ro)
            new PreparedMultiRenderObject(res.Children |> List.map add)

        let preparedObjects = objects |> ASet.mapUse prepareRenderObject
        let preparedObjectReader = preparedObjects.GetReader()

        let mutable subtasks = Map.empty

        let getSubTask (pass : RenderPass) : AbstractSubTask =
            match Map.tryFind pass subtasks with
                | Some task -> task
                | _ ->
                    let task = 
                        match pass.Order with
                            | RenderPassOrder.Arbitrary ->
                                new StaticOrderSubTask(this) :> AbstractSubTask

                            | order ->
                                new CameraSortedSubTask(order, this) :> AbstractSubTask

                    subtasks <- Map.add pass task subtasks
                    task

        override x.Update(fbo : Framebuffer) =
            let mutable stats = FrameStatistics.Zero
            let deltas = preparedObjectReader.GetDelta x

            x.ResourceManager.DrawBufferManager.Write(fbo)

            resourceUpdateWatch.Restart()
            stats <- stats + resources.Update(x)
            resourceUpdateWatch.Stop()

            match deltas with
                | [] -> ()
                | _ -> x.StructureChanged()

            for d in deltas do 
                match d with
                    | Add v ->
                        let task = getSubTask v.RenderPass
                        task.Add v
                    | Rem v ->
                        let task = getSubTask v.RenderPass
                        task.Remove v

            stats   

        override x.Perform(fbo) =
            primitivesGenerated.Restart()


            let mutable runStats = []
            for (_,t) in Map.toSeq subtasks do
                let s = t.Run(fbo)
                runStats <- s::runStats

            primitivesGenerated.Stop()

            GL.Sync()
            
            let primitives = primitivesGenerated.Value
            !this.Scope.stats + 
            (runStats |> List.sumBy (fun l -> l.Value)) +
            callStats.GetValue() +
            { FrameStatistics.Zero with
                ResourceUpdateSubmissionTime = resourceUpdateWatch.ElapsedCPU
                ResourceUpdateTime = resourceUpdateWatch.ElapsedGPU
                PrimitiveCount = float primitives
            }

        override x.Release() =
            preparedObjectReader.Dispose()
            resources.Dispose()
            for (_,t) in Map.toSeq subtasks do
                t.Dispose()

            subtasks <- Map.empty

    type ClearTask(runtime : IRuntime, fboSignature : IFramebufferSignature, color : IMod<list<Option<C4f>>>, depth : IMod<Option<float>>, ctx : Context) =
        inherit AdaptiveObject()

        let mutable frameId = 0UL

        member x.Run(caller : IAdaptiveObject, desc : OutputDescription) =
            let fbo = desc.framebuffer
            using ctx.ResourceLock (fun _ ->
                x.EvaluateAlways caller (fun () ->

                    let old = Array.create 4 0
                    let mutable oldFbo = 0
                    OpenTK.Graphics.OpenGL.GL.GetInteger(OpenTK.Graphics.OpenGL.GetPName.Viewport, old)
                    OpenTK.Graphics.OpenGL.GL.GetInteger(OpenTK.Graphics.OpenGL.GetPName.FramebufferBinding, &oldFbo)

                    let handle = fbo.GetHandle null |> unbox<int>

                    if ExecutionContext.framebuffersSupported then
                        GL.BindFramebuffer(OpenTK.Graphics.OpenGL4.FramebufferTarget.Framebuffer, handle)
                        GL.Check "could not bind framebuffer"
                    elif handle <> 0 then
                        failwithf "cannot render to texture on this OpenGL driver"

                    GL.Viewport(0, 0, fbo.Size.X, fbo.Size.Y)
                    GL.Check "could not bind framebuffer"

                    let depthValue = depth.GetValue x
                    let colorValues = color.GetValue x
                    
                    colorValues |> List.iteri (fun i _ ->
                        GL.ColorMask(i, true, true, true, true)
                    )
                    GL.DepthMask(true)
                    GL.StencilMask(0xFFFFFFFFu)

                    match colorValues, depthValue with
                        | [Some c], Some depth ->
                            GL.ClearColor(c.R, c.G, c.B, c.A)
                            GL.ClearDepth(depth)
                            GL.Clear(ClearBufferMask.ColorBufferBit ||| ClearBufferMask.DepthBufferBit ||| ClearBufferMask.StencilBufferBit)
                        
                        | [Some c], None ->
                            GL.ClearColor(c.R, c.G, c.B, c.A)
                            GL.Clear(ClearBufferMask.ColorBufferBit)

                        | l, Some depth when List.forall Option.isNone l ->
                            GL.ClearDepth(depth)
                            GL.Clear(ClearBufferMask.DepthBufferBit ||| ClearBufferMask.StencilBufferBit)
                        | l, d ->
                            
                            let mutable i = 0
                            for c in l do
                                match c with
                                    | Some c ->
                                        GL.DrawBuffer(int DrawBufferMode.ColorAttachment0 + i |> unbox)
                                        GL.ClearColor(c.R, c.G, c.B, c.A)
                                        GL.Clear(ClearBufferMask.ColorBufferBit)
                                    | None ->
                                        ()
                                i <- i + 1

                            match d with
                                | Some depth -> 
                                    GL.ClearDepth(depth)
                                    GL.Clear(ClearBufferMask.DepthBufferBit ||| ClearBufferMask.StencilBufferBit)
                                | None ->
                                    ()


                    if ExecutionContext.framebuffersSupported then
                        GL.BindFramebuffer(OpenTK.Graphics.OpenGL4.FramebufferTarget.Framebuffer, oldFbo)

                    GL.Viewport(old.[0], old.[1], old.[2], old.[3])
                    GL.Check "could not bind framebuffer"

                    frameId <- frameId + 1UL

                    RenderingResult(fbo, FrameStatistics.Zero)
                )
            )

        member x.Dispose() =
            color.RemoveOutput x
            depth.RemoveOutput x

        interface IRenderTask with
            member x.FramebufferSignature = fboSignature
            member x.Runtime = runtime |> Some
            member x.Run(caller, fbo) =
                x.Run(caller, fbo)

            member x.Dispose() =
                x.Dispose()

            member x.FrameId = frameId


    type TransformFeedbackRenderTask(man : ResourceManager, program : Program, wantedSemantics : list<Symbol>, mode : IndexedGeometryMode, objects : aset<IRenderObject>, config : IMod<BackendConfiguration>, shareTextures : bool, shareBuffers : bool) =
        inherit RenderTask(man, TransformFeedbackSignature(man.Context.Runtime, program, mode, wantedSemantics), objects, config, shareTextures, shareBuffers)
        let ctx = man.Context

        let fbo = Framebuffer.EmptyWithSignature base.FramebufferSignature

        let primitiveType =
            match mode with
                | IndexedGeometryMode.PointList -> 
                    TransformFeedbackPrimitiveType.Points 

                | IndexedGeometryMode.LineList | IndexedGeometryMode.LineStrip ->
                    TransformFeedbackPrimitiveType.Lines

                | IndexedGeometryMode.TriangleList | IndexedGeometryMode.TriangleStrip ->
                    TransformFeedbackPrimitiveType.Triangles

                | _ ->
                    failwithf "[GL] unsupported TransformFeedback primitive type: %A" mode

        let primitivesWritten = OpenGlQuery(QueryTarget.TransformFeedbackPrimitivesWritten)

        member x.Run (caller : IAdaptiveObject, buffers : Map<Symbol, Aardvark.Rendering.GL.Buffer * int64 * int64>) =
            x.EvaluateAlways caller (fun () ->
                x.OutOfDate <- true

                use token = ctx.ResourceLock 
                if x.CurrentContext.UnsafeCache <> ctx.CurrentContextHandle.Value then
                    transact (fun () -> Mod.change x.CurrentContext ctx.CurrentContextHandle.Value)

                let debugState = x.pushDebugOutput()

                let innerStats = 
                    x.RenderTaskLock.Run (fun () -> 
                        
                        let su = x.Update fbo
                        
                        // discard all primitives before rasterization
                        GL.Enable(EnableCap.RasterizerDiscard)
                        GL.Check "could not enable RasterizerDiscard"

                        let oldProgram = GL.GetInteger(GetPName.CurrentProgram)

                        // bind the desired program
                        GL.UseProgram(program.Handle)
                        GL.Check "could not use program"
           
                        // bind the feedback buffers
                        let mutable index = 0
                        for sem in wantedSemantics do
                            match Map.tryFind sem buffers with
                                | Some(buffer, offset, size) ->
                                    GL.BindBufferRange(BufferRangeTarget.TransformFeedbackBuffer, index, buffer.Handle, nativeint offset, nativeint size)
                                | None ->
                                    GL.BindBufferBase(BufferRangeTarget.TransformFeedbackBuffer, index, 0)
                            index <- index + 1

                        GL.Check "could not bind buffer"

                        // start the feedback process
                        primitivesWritten.Restart()
                        GL.BeginTransformFeedback(primitiveType); GL.Check "could not start TransformFeedback"

                        // run the render-commands
                        let sp = x.Perform fbo

                        // stop the feedback process
                        GL.EndTransformFeedback(); GL.Check "could stop TransformFeedback"
                        primitivesWritten.Stop()

                        // unbind the feedback buffers
                        for i in 0..index-1 do
                            GL.BindBufferBase(BufferRangeTarget.TransformFeedbackBuffer, i, 0)
                        GL.Check "could not unbind buffer"


                        // re-enable rasterization
                        GL.Disable(EnableCap.RasterizerDiscard)
                        GL.Check "could not disable RasterizerDiscard"

                        // unbind the program
                        GL.UseProgram(oldProgram)
                        GL.Check "could not unbind program"
                        

                        // sync in order to get the feedback count
                        GL.Sync()


                        let cnt = primitivesWritten.Value
                        { (su + sp) with PrimitiveCount = float cnt }
                    )

                x.popDebugOutput debugState

                
                x.incrementFrameId()

                innerStats + !x.Scope.stats
            )

        member x.Mode = mode
        member x.WantedSemantics = wantedSemantics

        override x.Release() =
            base.Release()
            primitivesWritten.Reset()
            ctx.Delete(program)

        interface ITransformFeedbackRenderTask with
            member x.Surface = program :> IBackendSurface
            member x.Run(caller, buffers) = x.Run(caller, buffers |> Map.map (fun _ (b,o,s) -> unbox b, o, s))
            member x.Mode = mode
            member x.WantedSemantics = wantedSemantics