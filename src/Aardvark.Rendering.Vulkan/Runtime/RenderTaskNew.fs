namespace Aardvark.Rendering.Vulkan

open System
open System.Threading
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Aardvark.Base
open Aardvark.Base.Rendering
open Aardvark.Rendering.Vulkan
open Microsoft.FSharp.NativeInterop
open Aardvark.Base.Incremental
open System.Diagnostics
open System.Collections.Generic
open Aardvark.Base.Runtime

#nowarn "9"
#nowarn "51"

type ICommandStreamResource =
    inherit IResourceLocation<VKVM.CommandStream>
    abstract member Stream : VKVM.CommandStream
    abstract member Resources : seq<IResourceLocation>

    abstract member GroupKey : list<obj>
    abstract member BoundingBox : IMod<Box3d>

module RenderCommands =
 
    [<RequireQualifiedAccess>]
    type Tree<'a> =
        | Empty
        | Leaf of 'a
        | Node of Option<'a> * list<Tree<'a>> 

     
    type ClearValues =
        {
            colors  : Map<Symbol, IMod<C4f>>
            depth   : Option<IMod<float>>
            stencil : Option<IMod<int>>
        }

    type PipelineState =
        {
            surface             : Aardvark.Base.Surface

            depthTest           : IMod<DepthTestMode>
            cullMode            : IMod<CullMode>
            blendMode           : IMod<BlendMode>
            fillMode            : IMod<FillMode>
            stencilMode         : IMod<StencilMode>
            multisample         : IMod<bool>
            writeBuffers        : Option<Set<Symbol>>
            globalUniforms      : IUniformProvider

            geometryMode        : IndexedGeometryMode
            vertexInputTypes    : Map<Symbol, Type>
            perGeometryUniforms : Map<string, Type>
        }
   

    type Geometry =
        {
            vertexAttributes    : Map<Symbol, IMod<IBuffer>>
            indices             : Option<Aardvark.Base.BufferView>
            uniforms            : Map<string, IMod>
            call                : IMod<list<DrawCallInfo>>
        }

    type TreeRenderObject(pipe : PipelineState, geometries : IMod<Tree<Geometry>>) =
        let id = newId()

        member x.Pipeline = pipe
        member x.Geometries = geometries

        interface IRenderObject with
            member x.AttributeScope = Ag.emptyScope
            member x.Id = id
            member x.RenderPass = RenderPass.main
            

    type RenderCommand =
        | Objects of aset<IRenderObject>
        | ViewDependent of pipeline : PipelineState * (Trafo3d -> Trafo3d -> list<Geometry>)

    type Command = 
        | Render of objects : aset<IRenderObject>
        | Clear of values : ClearValues
        | Blit of sourceAttachment : Symbol * target : IBackendTextureOutputView

    type PreparedPipelineState =
        {
            ppPipeline  : INativeResourceLocation<VkPipeline>
            ppLayout    : PipelineLayout
        }

    type PreparedGeometry =
        {
            pgOriginal      : Geometry
            pgDescriptors   : INativeResourceLocation<DescriptorSetBinding>
            pgAttributes    : INativeResourceLocation<VertexBufferBinding>
            pgIndex         : Option<INativeResourceLocation<IndexBufferBinding>>
            pgCall          : INativeResourceLocation<DrawCall>
            pgResources     : list<IResourceLocation>
        }

    type NoCache private() =
        static let instance = NoCache() :> IResourceCache

        static member Instance = instance

        interface IResourceUser with
            member x.AddLocked _ = ()
            member x.RemoveLocked _ = ()
        interface IResourceCache with
            member x.Remove _ = ()

    type PreparedFragment(resources : list<IResourceLocation>, compile : VKVM.CommandStream -> unit) =
        inherit AbstractResourceLocation<VKVM.CommandStream>(NoCache.Instance, [])

        let mutable isCreated = false

        let mutable stream = Unchecked.defaultof<VKVM.CommandStream>
        let mutable prev : Option<PreparedFragment> = None
        let mutable next : Option<PreparedFragment> = None
        let mutable tag : obj = null

        member x.Resources = resources

        member x.Tag
            with get() : obj = tag
            and set (t : obj) = tag <- t

        member x.Prev
            with get() = prev
            and set (p : Option<PreparedFragment>) = 
                prev <- p

        member x.Next
            with get() = next
            and set (n : Option<PreparedFragment>) = 
                next <- n
                match n with
                    | Some n -> stream.Next <- Some n.Stream
                    | None -> stream.Next <- None

        override x.Create() =
            stream <- new VKVM.CommandStream()
            compile stream
            isCreated <- true

        override x.Destroy() =
            stream.Dispose()
            stream <- Unchecked.defaultof<VKVM.CommandStream>
            isCreated <- false

        override x.GetHandle(t) =
            { handle = stream; version = 0 }

        member x.Stream : VKVM.CommandStream = stream

        interface ILinked<PreparedFragment> with
            member x.Prev
                with get() = x.Prev
                and set p = x.Prev <- p

            member x.Next
                with get() = x.Next
                and set n = x.Next <- n

    let private noStats =
        let stats = NativePtr.alloc 1
        NativePtr.write stats V2i.Zero
        stats

    let private alwaysActive =
        let active = NativePtr.alloc 1
        NativePtr.write active 1
        active

    type ResourceManager with
        member x.PreparePipelineState (renderPass : RenderPass, state : PipelineState) =
            let layout, program = x.CreateShaderProgram(renderPass, state.surface)

            let inputs = 
                layout.PipelineInfo.pInputs |> List.map (fun p ->
                    let name = Symbol.Create p.name
                    match Map.tryFind name state.vertexInputTypes with
                        | Some t -> (name, (false, t))
                        | None -> failf "could not get shader input %A" name
                )
                |> Map.ofList

            let inputState =
                x.CreateVertexInputState(layout.PipelineInfo, Mod.constant (VertexInputState.ofTypes inputs))

            let inputAssembly =
                x.CreateInputAssemblyState(Mod.constant state.geometryMode)

            let rasterizerState =
                x.CreateRasterizerState(state.depthTest, state.cullMode, state.fillMode)

            let colorBlendState =
                x.CreateColorBlendState(renderPass, state.writeBuffers, state.blendMode)

            let depthStencil =
                let depthWrite = 
                    match state.writeBuffers with
                        | None -> true
                        | Some s -> Set.contains DefaultSemantic.Depth s
                x.CreateDepthStencilState(depthWrite, state.depthTest, state.stencilMode)

            let pipeline = 
                x.CreatePipeline(
                    program,
                    renderPass,
                    inputState,
                    inputAssembly,
                    rasterizerState,
                    colorBlendState,
                    depthStencil,
                    state.writeBuffers
                )
            {
                ppPipeline  = pipeline
                ppLayout    = layout
            }

        member x.PrepareGeometry(state : PreparedPipelineState, g : Geometry) : PreparedGeometry =
            let resources = System.Collections.Generic.List<IResourceLocation>()

            let layout = state.ppLayout

            let descriptorSets, additionalResources = 
                x.CreateDescriptorSets(layout, UniformProvider.ofMap g.uniforms)

            resources.AddRange additionalResources

            let vertexBuffers = 
                layout.PipelineInfo.pInputs 
                    |> List.sortBy (fun i -> i.location) 
                    |> List.map (fun i ->
                        let sem = i.semantic 
                        match Map.tryFind sem g.vertexAttributes with
                            | Some b ->
                                x.CreateBuffer(b), 0L
                            | None ->
                                failf "geometry does not have buffer %A" sem
                    )

            let dsb = x.CreateDescriptorSetBinding(layout, Array.toList descriptorSets)
            let vbb = x.CreateVertexBufferBinding(vertexBuffers)

            let isIndexed, ibo =
                match g.indices with
                    | Some ib ->
                        let b = x.CreateIndexBuffer ib.Buffer
                        let ibb = x.CreateIndexBufferBinding(b, VkIndexType.ofType ib.ElementType)
                        resources.Add ibb
                        true, ibb |> Some
                    | None ->
                        false, None

            let call = x.CreateDrawCall(isIndexed, g.call)

            resources.Add dsb
            resources.Add vbb
            resources.Add call



            {
                pgOriginal      = g
                pgDescriptors   = dsb
                pgAttributes    = vbb
                pgIndex         = ibo
                pgCall          = call
                pgResources     = CSharpList.toList resources
            }

        member x.PrepareFragment(state : PreparedPipelineState, g : Geometry) : PreparedFragment =
            let pg = x.PrepareGeometry(state, g)

            let compile (stream : VKVM.CommandStream) =
                stream.IndirectBindDescriptorSets(pg.pgDescriptors.Pointer) |> ignore
                stream.IndirectBindVertexBuffers(pg.pgAttributes.Pointer) |> ignore
                match pg.pgIndex with
                    | Some ibb -> stream.IndirectBindIndexBuffer(ibb.Pointer) |> ignore
                    | None -> ()
                stream.IndirectDraw(noStats, alwaysActive, pg.pgCall.Pointer) |> ignore

            let f = PreparedFragment(pg.pgResources, compile)
            f.Tag <- g
            f

    [<AllowNullLiteral>]
    type MNode<'a, 'b when 'b :> ILinked<'b>> =
        class
            val mutable public Parent : MTree<'a, 'b>
            val mutable public Value : Option<'b>
            val mutable public Children : MNode<'a, 'b>[]

            val mutable public First : 'b
            val mutable public Last : 'b

            new(p : MTree<'a, 'b>, v : 'b) = 
                { Parent = p; Value = Some v; Children = [||]; First = v; Last = v }

            new(p : MTree<'a, 'b>, v : Option<'b>, c : MNode<'a, 'b>[], first : 'b, last : 'b) = 
                { Parent = p; Value = v; Children = c; First = first; Last = last }
        end


    and MTree<'a, 'b when 'b :> ILinked<'b>>(invoke : 'a -> 'b, revoke : 'b -> unit, identical : 'b -> 'a -> bool) =

        let mutable first : Option<'b> = None
        let mutable last : Option<'b> = None
        let mutable root : MNode<'a, 'b> = null

        let rec create (x : MTree<'a, 'b>) (t : Tree<'a>) =
            match t with
                | Tree.Empty -> 
                    null
                | Tree.Leaf v ->
                    MNode(x, invoke v)
                | Tree.Node(s,c) ->
                    let c = List.toArray c

                    let s = s |> Option.map invoke

                    let mutable first = s
                    let mutable last = s
                    let mc = Array.zeroCreate c.Length
                    for i in 0 .. c.Length - 1 do
                        let ci = create x c.[i]
                        match ci with
                            | null -> ()
                            | ci ->
                                match last with
                                    | Some l -> 
                                        l.Next <- Some ci.First
                                        last <- Some ci.Last
                                    | None ->
                                        first <- Some ci.First
                                        last <- Some ci.Last
                        mc.[i] <- ci

                    MNode(x, s, mc, first.Value, last.Value)

        let rec destroy (m : MNode<'a, 'b>) =
            match m with
                | null -> ()
                | _ ->
                    m.Value |> Option.iter revoke
                    m.Children |> Array.iter destroy

        let getFirst (m : MNode<'a, 'b>) =
            match m with
                | null -> None
                | n -> Some n.First

        let getLast (m : MNode<'a, 'b>) =
            match m with
                | null -> None
                | n -> Some n.Last

        let replace (o : 'b) (n : 'b) =
            let prev = o.Prev
            let next = o.Next
            match prev with
                | Some pp -> pp.Next <- Some n
                | None -> first <- Some n

            match next with
                | Some nn -> nn.Prev <- Some n
                | None -> last <- Some n

        let insert (p : Option<'b>) (v : 'b) (n : Option<'b>) =
            match p with
                | Some p -> assert(Unchecked.equals p.Next n); p.Next <- Some v
                | None -> first <- Some v

            match n with
                | Some n -> n.Prev <- Some v
                | None -> last <- Some v
        
        let remove (b : 'b) =
            let p = b.Prev
            let n = b.Next

            match p with
                | Some p -> p.Next <- n
                | None -> first <- n

            match n with
                | Some n -> n.Prev <- p
                | None -> last <- p

        member private x.Destroy(t : MNode<'a, 'b>) =
            match t with
                | null -> 
                    ()
                | _ ->
                    let p = t.First.Prev
                    let n = t.Last.Next

                    destroy t

                    match p with
                        | Some p -> p.Next <- n
                        | None -> first <- n

                    match n with
                        | Some n -> n.Prev <- p
                        | None -> last <- p

        member private x.Update(this : byref<MNode<'a, 'b>>, prev : Option<'b>, t : Tree<'a>, next : Option<'b>) =  
            match this, t with
                | null, Tree.Empty ->
                    ()

                | null, t -> 
                    let m = create x t

                    match prev with
                        | Some p -> p.Next <- Some m.First
                        | None -> first <- Some m.First

                    match next with
                        | Some n -> n.Prev <- Some m.Last
                        | None -> last <- Some m.Last

                    this <- m


                | o, Tree.Empty ->
                    x.Destroy o
                    this <- null
                
                | o, Tree.Leaf a ->
                    if o.Children.Length > 0 then
                        let fc = o.Children.[0].First
                        let lc = o.Children.[o.Children.Length - 1].Last
                        o.Children |> Array.iter destroy
                        o.Children <- [||]

                        let p = fc.Prev
                        let n = lc.Next
                        match p with
                            | Some p -> p.Next <- n
                            | None -> first <- n 

                        match n with
                            | Some n -> n.Prev <- p
                            | None -> last <- p 

                    match o.Value with
                        | Some b -> 
                            if not (identical b a) then
                                let n = invoke a
                                replace b n
                                revoke b
                                o.Value <- Some n
                        | None ->
                            let b = invoke a
                            insert prev b next
                            o.Value <- Some b


                | o, Tree.Node(s,c) ->
                    let mutable prev = prev

                    match s with    
                        | Some a -> 
                            match o.Value with
                                | Some b -> 
                                    if not (identical b a) then
                                        let n = invoke a
                                        replace b n
                                        revoke b
                                        o.Value <- Some n
                                        prev <- Some n
                                    else
                                        prev <- Some b
                                | None ->
                                    let b = invoke a
                                    insert prev b next
                                    o.Value <- Some b
                                    prev <- Some b
                        | None ->
                            match o.Value with
                                | Some b -> 
                                    remove b
                                    revoke b
                                    o.Value <- None
                                | None -> 
                                    ()

                    let c = List.toArray c

                    if o.Children.Length < c.Length then
                        Array.Resize(&o.Children, c.Length)

                    elif o.Children.Length > c.Length then
                        for i in c.Length .. o.Children.Length - 1 do
                            x.Destroy(o.Children.[i])
                        Array.Resize(&o.Children, c.Length)

                    for i in 0 .. c.Length - 1 do
                        let n = 
                            if i < c.Length - 1 then getFirst o.Children.[i+1]
                            else next

                        x.Update(&o.Children.[i], prev, c.[i], n)

                        match getLast o.Children.[i] with
                            | Some l -> prev <- Some l
                            | None -> ()

        member x.First = first
        member x.Last = last

        member x.Update(t : Tree<'a>) =
            x.Update(&root, None, t, None)

//
//    type MTreeEntry =
//        {
//            preparedGeometry : PreparedGeometry
//            stream : VKVM.CommandStream
//        }
//
//    [<AllowNullLiteral>]
//    type GeometryNode(parent : GeometryTree) =
//        let mutable children : array<GeometryNode> = [||]
//        let mutable value : Option<MTreeEntry> = None
//
//        let mutable prev : GeometryNode = null
//        let mutable next : GeometryNode = null
//        let mutable original : Tree<Geometry> = Tree.Empty
//
//        member x.Prev
//            with get() = prev
//            and set p = prev <- p
//
//        member x.Next
//            with get() = next
//            and set n = next <- n
//
//        member x.Children
//            with get() = children
//            and set c = children <- c
//
//        [<CompilationRepresentation(CompilationRepresentationFlags.Static)>]
//        member x.First =
//            match x with
//                | null -> None
//                | _ ->
//                    match value with    
//                        | Some v -> Some v.stream
//                        | None -> 
//                            if children.Length = 0 then None
//                            else children.[0].First
//                    
//        [<CompilationRepresentation(CompilationRepresentationFlags.Static)>]
//        member x.Last =
//            match x with
//                | null -> None
//                | _ ->
//                    if children.Length = 0 then
//                        match value with    
//                            | Some v -> Some v.stream
//                            | None -> None
//                    else
//                        children.[children.Length - 1].Last
//
//        member x.Value
//            with get() = value
//            and set (v : Option<MTreeEntry>) =
//                match value, v with
//                    | None, Some v -> 
//                        let prev =
//                            match prev.Last with
//                                | Some p -> Some p
//                                | None ->
//                                    match parent.First with
//                                        | Some p -> Some p
//                                        | None -> None
//
//                        match prev with
//                            | Some p ->
//                                let pn = p.Next
//                                v.stream.Next <- pn
//                                p.Next <- Some v.stream
//                            | None ->
//                                parent.First <- Some v.stream
//                    | Some v, None ->
//                        let p = v.stream.Prev
//                        let n = v.stream.Next
//                        match p with
//                            | Some p -> p.Next <- n
//                            | None -> parent.First <- n
//
//                    | Some oe, Some ne ->
//                        if not (System.Object.ReferenceEquals(oe,ne)) then
//                            let p = oe.stream.Prev
//                            let n = oe.stream.Next
//
//                            ne.stream.Next <- n
//                            match p with
//                                | Some p -> p.Next <- Some ne.stream
//                                | None -> parent.First <- Some ne.stream
//
//                        ()
//                        
//                    | None, None ->
//                        ()
//
//
//
//    and GeometryTree() =
//        let mutable first : Option<VKVM.CommandStream> = None
//        let mutable last : Option<VKVM.CommandStream> = None
//
//        member x.First
//            with get() = first
//            and set f = first <- f
//
//        member x.Last
//            with get() = last
//            and set l = last <- l
//
//    and [<RequireQualifiedAccess>] GeometryRef =
//        | Empty
//        | Value of VKVM.CommandStream
//        | Node of GeometryNode
//
//        member x.First =
//            match x with
//                | GeometryRef.Empty | GeometryRef.Node null -> None
//                | GeometryRef.Value v -> Some v
//                | GeometryRef.Node t -> 
//                    match t.Value with
//                        | Some v -> Some v.stream
//                        | None ->
//                            if t.Children.Length = 0 then
//                                (GeometryRef.Node t.Next).First
//                            else
//                                t.Children.[0].First
//
//        member x.Last =
//            match x with
//                | GeometryRef.Empty | GeometryRef.Node null -> None
//                | GeometryRef.Value v -> Some v
//                | GeometryRef.Node t ->
//                    if t.Children.Length = 0 then
//                        match t.Value with
//                            | Some v -> Some v.stream
//                            | None -> (GeometryRef.Node t.Prev).Last
//                    else
//                        t.Children.[t.Children.Length - 1].Last
//
//    [<AllowNullLiteral>]
//    type MTree =
//        class
//            val mutable public Children : list<ref<MTree>>
//            val mutable public Value : Option<MTreeEntry>
//
//            new(value) = { Value = Some value; Children = [] }
//            new(value, children) = { Value = value; Children = children }
//        end
//
//    type MTree with
//        member x.IsEmpty =
//            match x with
//                | null -> true
//                | _ -> false
//
//        member x.IsNode =
//            if isNull x then 
//                false
//            else
//                match x.Children with
//                    | [] -> false
//                    | _ -> true
//        
//    let inline (|MEmpty|MLeaf|MNode|) (t : MTree) =
//        match t with
//            | null -> MEmpty
//            | _ ->
//                match t.Children with
//                    | [] -> MLeaf t.Value.Value
//                    | c -> MNode(t.Value, t.Children)
//


    type TreeCommandStreamResource(owner, key, pipe : PipelineState, things : IMod<Tree<Geometry>>, resources : ResourceSet, manager : ResourceManager, renderPass : RenderPass, stats : nativeptr<V2i>) =
        inherit AbstractResourceLocation<VKVM.CommandStream>(owner, key)
         
        let id = newId()

        let mutable stream = Unchecked.defaultof<VKVM.CommandStream>
        let mutable entry = Unchecked.defaultof<VKVM.CommandStream>
        let preparedPipeline = manager.PreparePipelineState(renderPass, pipe)

        let bounds = lazy (Mod.constant Box3d.Invalid)
        let allResources = ReferenceCountingSet<IResourceLocation>()

        let isActive =
            let isActive = NativePtr.alloc 1
            NativePtr.write isActive 1
            isActive

        let prepare (g : Geometry) =
            let p = manager.PrepareFragment(preparedPipeline, g)
            for r in p.Resources do 
                if allResources.Add r then 
                    resources.AddAndUpdate r
            p.Acquire()
            p

        let release (e : PreparedFragment) =
            e.Release()
            for r in e.Resources do
                if allResources.Remove r then 
                    resources.Remove r

        let isIdentical (e : PreparedFragment) (o : Geometry) =
            System.Object.ReferenceEquals(e.Tag, o)

        let state = MTree<Geometry, PreparedFragment>(prepare, release, isIdentical)

        let performUpdate (t : Tree<Geometry>) =
            state.Update(t)
            entry.Next <- state.First |> Option.map (fun f -> f.Stream)

        member x.Stream = stream
        member x.GroupKey = [preparedPipeline.ppPipeline :> obj; id :> obj]
        member x.BoundingBox = bounds.Value

        interface ICommandStreamResource with
            member x.Stream = x.Stream
            member x.Resources = allResources :> seq<_>
            member x.GroupKey = x.GroupKey
            member x.BoundingBox = x.BoundingBox

        override x.Create() =
            if allResources.Add preparedPipeline.ppPipeline then resources.Add preparedPipeline.ppPipeline

            stream <- new VKVM.CommandStream()
            entry <- new VKVM.CommandStream()
            stream.Call(entry) |> ignore
            entry.IndirectBindPipeline preparedPipeline.ppPipeline.Pointer |> ignore
            
        override x.Destroy() = 
            stream.Dispose()
            entry.Dispose()
            for r in allResources do resources.Remove r
            allResources.Clear()
            
        override x.GetHandle token =
            let tree = things.GetValue token
            performUpdate tree
            { handle = stream; version = 0 }   



module RenderTaskNew =

    [<AbstractClass; Sealed; Extension>]
    type IRenderObjectExts private() =
        [<Extension>]
        static member ComputeBoundingBox (o : IRenderObject) : IMod<Box3d> =
            match o with
                | :? RenderObject as o ->
                    match Ag.tryGetAttributeValue o.AttributeScope "GlobalBoundingBox" with
                        | Success box -> box
                        | _ -> failwith "[Vulkan] could not get BoundingBox for RenderObject"
                    
                | :? MultiRenderObject as o ->
                    o.Children |> List.map IRenderObjectExts.ComputeBoundingBox |> Mod.mapN Box3d

                | :? PreparedMultiRenderObject as o ->
                    o.Children |> List.map IRenderObjectExts.ComputeBoundingBox |> Mod.mapN Box3d
                    
                | :? PreparedRenderObject as o ->
                    IRenderObjectExts.ComputeBoundingBox o.original

                | _ ->
                    failf "invalid renderobject %A" o

    module CommandStreams = 
        type CommandStreamResource(owner, key, o : IRenderObject, resources : ResourceSet, manager : ResourceManager, renderPass : RenderPass, stats : nativeptr<V2i>) =
            inherit AbstractResourceLocation<VKVM.CommandStream>(owner, key)
         
            let mutable stream = Unchecked.defaultof<VKVM.CommandStream>
            let mutable prep : PreparedMultiRenderObject = Unchecked.defaultof<_>

            let compile (o : IRenderObject) =
                let o = manager.PrepareRenderObject(renderPass, o)
                for o in o.Children do
                    for r in o.resources do resources.Add r
                                
                    stream.IndirectBindPipeline(o.pipeline.Pointer) |> ignore
                    stream.IndirectBindDescriptorSets(o.descriptorSets.Pointer) |> ignore

                    match o.indexBuffer with
                        | Some ib ->
                            stream.IndirectBindIndexBuffer(ib.Pointer) |> ignore
                        | None ->
                            ()

                    stream.IndirectBindVertexBuffers(o.vertexBuffers.Pointer) |> ignore
                    stream.IndirectDraw(stats, o.isActive.Pointer, o.drawCalls.Pointer) |> ignore
                o



            let bounds = lazy (o.ComputeBoundingBox())


            member x.Stream = stream
            member x.Object = prep
            member x.GroupKey = [prep.Children.[0].pipeline :> obj; prep.Id :> obj]
            member x.BoundingBox = bounds.Value

            interface ICommandStreamResource with
                member x.Stream = x.Stream
                member x.Resources = prep.Children |> Seq.collect (fun c -> c.resources)
                member x.GroupKey = x.GroupKey
                member x.BoundingBox = x.BoundingBox

            override x.Create() =
                stream <- new VKVM.CommandStream()
                let p = compile o
                prep <- p

            override x.Destroy() = 
                stream.Dispose()
                for o in prep.Children do
                    for r in o.resources do resources.Remove r
                prep <- Unchecked.defaultof<_>

            override x.GetHandle _ = 
                { handle = stream; version = 0 }   

    module Compiler = 
        open CommandStreams

        type RenderObjectCompiler(manager : ResourceManager, renderPass : RenderPass) =
            inherit ResourceSet()

            let stats : nativeptr<V2i> = NativePtr.alloc 1
            let cache = ResourceLocationCache<VKVM.CommandStream>(manager.ResourceUser)
            let mutable version = 0

            override x.InputChanged(t ,i) =
                base.InputChanged(t, i)
                match i with
                    | :? IResourceLocation<UniformBuffer> -> ()
                    | :? IResourceLocation -> version <- version + 1
                    | _ -> ()
 
            member x.Dispose() =
                cache.Clear()

            member x.Compile(o : IRenderObject) : ICommandStreamResource =
                let call = 
                    cache.GetOrCreate([o :> obj], fun owner key ->
                        match o with
                            | :? RenderCommands.TreeRenderObject as o ->
                                new RenderCommands.TreeCommandStreamResource(owner, key, o.Pipeline, o.Geometries, x, manager, renderPass, stats) :> ICommandStreamResource
                            | _ -> 
                                new CommandStreamResource(owner, key, o, x, manager, renderPass, stats) :> ICommandStreamResource
                    )
                call.Acquire()
                call |> unbox<ICommandStreamResource>
            
            member x.CurrentVersion = version

    module ChangeableCommandBuffers = 
        open Compiler

        [<AbstractClass>]
        type AbstractChangeableCommandBuffer(manager : ResourceManager, pool : CommandPool, renderPass : RenderPass, viewports : IMod<Box2i[]>) =
            inherit Mod.AbstractMod<CommandBuffer>()

            let device = pool.Device
            let compiler = RenderObjectCompiler(manager, renderPass)
            let mutable resourceVersion = 0
            let mutable cmdVersion = -1
            let mutable cmdViewports = [||]

            let cmdBuffer = pool.CreateCommandBuffer(CommandBufferLevel.Secondary)
            let dirty = HashSet<ICommandStreamResource>()

            abstract member Release : unit -> unit
            abstract member Prolog : VKVM.CommandStream
            abstract member Sort : AdaptiveToken -> bool
            default x.Sort _ = false

            override x.InputChanged(t : obj, o : IAdaptiveObject) =
                match o with
                    | :? ICommandStreamResource as r ->
                        lock dirty (fun () -> dirty.Add r |> ignore)
                    | _ ->
                        ()

            member x.Compile(o : IRenderObject) =
                let res = compiler.Compile(o)
                lock x (fun () ->
                    let o = x.OutOfDate
                    try x.EvaluateAlways AdaptiveToken.Top (fun t -> res.Update(t) |> ignore; res)
                    finally x.OutOfDate <- o
                )

            member x.Changed() =
                cmdVersion <- -1
                x.MarkOutdated()
            

            member x.Destroy(r : ICommandStreamResource) =
                lock dirty (fun () -> dirty.Remove r |> ignore)
                r.Release()

            member x.Dispose() =
                compiler.Dispose()
                dirty.Clear()
                cmdBuffer.Dispose()

            override x.Compute (t : AdaptiveToken) =
                // update all dirty programs 
                let dirty =
                    lock dirty (fun () ->
                        let res = dirty |> HashSet.toArray
                        dirty.Clear()
                        res
                    )

                for d in dirty do
                    d.Update(t) |> ignore

                // update all resources
                compiler.Update t |> ignore
                resourceVersion <- compiler.CurrentVersion

                // refill the CommandBuffer (if necessary)
                let vps = viewports.GetValue t
                let contentChanged      = cmdVersion < 0 || dirty.Length > 0
                let viewportChanged     = cmdViewports <> vps
                let versionChanged      = cmdVersion >= 0 && resourceVersion <> cmdVersion
                let orderChanged        = x.Sort t

                if contentChanged || versionChanged || viewportChanged || orderChanged then
                    let first = x.Prolog
                    let cause =
                        String.concat "; " [
                            if contentChanged then yield "content"
                            if versionChanged then yield "resources"
                            if viewportChanged then yield "viewport"
                            if orderChanged then yield "order"
                        ]
                        |> sprintf "{ %s }"

                    Log.line "[Vulkan] recompile commands: %s" cause
                    cmdViewports <- vps
                    cmdVersion <- resourceVersion

                    if viewportChanged then
                        first.SeekToBegin()
                        first.SetViewport(0u, vps |> Array.map (fun b -> VkViewport(float32 b.Min.X, float32 b.Min.X, float32 (1 + b.SizeX), float32 (1 + b.SizeY), 0.0f, 1.0f))) |> ignore
                        first.SetScissor(0u, vps |> Array.map (fun b -> VkRect2D(VkOffset2D(b.Min.X, b.Min.X), VkExtent2D(1 + b.SizeX, 1 + b.SizeY)))) |> ignore

                    cmdBuffer.Reset()
                    cmdBuffer.Begin(renderPass, CommandBufferUsage.RenderPassContinue)
                    cmdBuffer.AppendCommand()
                    first.Run(cmdBuffer.Handle)
                    cmdBuffer.End()

                cmdBuffer
            
        [<AbstractClass>]
        type AbstractChangeableSetCommandBuffer(manager : ResourceManager, pool : CommandPool, renderPass : RenderPass, viewports : IMod<Box2i[]>) =
            inherit AbstractChangeableCommandBuffer(manager, pool, renderPass, viewports)

            abstract member Add : IRenderObject -> bool
            abstract member Remove : IRenderObject -> bool

        type ChangeableUnorderedCommandBuffer(manager : ResourceManager, pool : CommandPool, renderPass : RenderPass, viewports : IMod<Box2i[]>) =
            inherit AbstractChangeableSetCommandBuffer(manager, pool, renderPass, viewports)

            let first = new VKVM.CommandStream()
            let trie = Trie<VKVM.CommandStream>()
            do trie.Add([], first)

            let cache = Dict<IRenderObject, ICommandStreamResource>()
            override x.Prolog = first

            override x.Release() =
                cache.Clear()
                first.Dispose()
                trie.Clear()

            override x.Add(o : IRenderObject) =
                if not (cache.ContainsKey o) then
                    let resource = x.Compile o
                    let key = resource.GroupKey
                    trie.Add(key, resource.Stream)
                    cache.[o] <- resource
                    x.Changed()
                    true
                else
                    false

            override x.Remove(o : IRenderObject) =
                match cache.TryRemove o with
                    | (true, r) ->
                        let key = r.GroupKey
                        trie.Remove key |> ignore
                        x.Destroy r 
                        x.Changed()
                        true
                    | _ ->
                        false

        type ChangeableOrderedCommandBuffer(manager : ResourceManager, pool : CommandPool, renderPass : RenderPass, viewports : IMod<Box2i[]>, sorter : IMod<Trafo3d -> Box3d[] -> int[]>) =
            inherit AbstractChangeableSetCommandBuffer(manager, pool, renderPass, viewports)
        
            let first = new VKVM.CommandStream()

            let cache = Dict<IRenderObject, IMod<Box3d> * ICommandStreamResource>()

            let mutable camera = Mod.constant Trafo3d.Identity


            override x.Add(o : IRenderObject) =
                if not (cache.ContainsKey o) then
                    if cache.Count = 0 then
                        match Ag.tryGetAttributeValue o.AttributeScope "ViewTrafo" with
                            | Success trafo -> camera <- trafo
                            | _ -> failf "could not get camera view"

                    let res = x.Compile o
                    let bb = res.BoundingBox
                    cache.[o] <- (bb, res)
                    x.Changed()
                    true
                else
                    false

            override x.Remove(o : IRenderObject) =
                match cache.TryRemove o with
                    | (true, (_,res)) -> 
                        x.Destroy res
                        x.Changed()
                        true
                    | _ -> 
                        false

            override x.Prolog = first

            override x.Release() =
                first.Dispose()

            override x.Sort t =
                let sorter = sorter.GetValue t
                let all = cache.Values |> Seq.toArray

                let boxes = Array.zeroCreate all.Length
                let streams = Array.zeroCreate all.Length
                for i in 0 .. all.Length - 1 do
                    let (bb, s) = all.[i]
                    let bb = bb.GetValue t
                    boxes.[i] <- bb
                    streams.[i] <- s.Stream

                let viewTrafo = camera.GetValue t
                let perm = sorter viewTrafo boxes
                let mutable last = first
                for i in perm do
                    let s = streams.[i]
                    last.Next <- Some s
                last.Next <- None


                true

    open ChangeableCommandBuffers

    type RenderTask(device : Device, renderPass : RenderPass, shareTextures : bool, shareBuffers : bool) =
        inherit AbstractRenderTask()

        let pool = device.GraphicsFamily.CreateCommandPool()
        let passes = SortedDictionary<Aardvark.Base.Rendering.RenderPass, AbstractChangeableSetCommandBuffer>()
        let viewports = Mod.init [||]
        
        let cmd = pool.CreateCommandBuffer(CommandBufferLevel.Primary)

        let locks = ReferenceCountingSet<ILockedResource>()

        let user =
            { new IResourceUser with
                member x.AddLocked l = lock locks (fun () -> locks.Add l |> ignore)
                member x.RemoveLocked l = lock locks (fun () -> locks.Remove l |> ignore)
            }

        let manager = new ResourceManager(user, device)

        static let sortByCamera (order : RenderPassOrder) (trafo : Trafo3d) (boxes : Box3d[]) =
            let sign = 
                match order with
                    | RenderPassOrder.BackToFront -> 1
                    | RenderPassOrder.FrontToBack -> -1
                    | _ -> failf "invalid order %A" order

            let compare (l : Box3d) (r : Box3d) =
                let l = trafo.Forward.TransformPos l.Center
                let r = trafo.Forward.TransformPos r.Center
                sign * compare l.Z r.Z


            boxes.CreatePermutationQuickSort(Func<_,_,_>(compare))

        member x.Add(o : IRenderObject) =
            let key = o.RenderPass
            let cmd =
                match passes.TryGetValue key with
                    | (true,c) -> c
                    | _ ->
                        let c = 
                            match key.Order with
                                | RenderPassOrder.BackToFront | RenderPassOrder.FrontToBack -> 
                                    ChangeableOrderedCommandBuffer(manager, pool, renderPass, viewports, Mod.constant (sortByCamera key.Order)) :> AbstractChangeableSetCommandBuffer
                                | _ -> 
                                    ChangeableUnorderedCommandBuffer(manager, pool, renderPass, viewports) :> AbstractChangeableSetCommandBuffer
                        passes.[key] <- c
                        x.MarkOutdated()
                        c
            cmd.Add(o)

        member x.Remove(o : IRenderObject) =
            let key = o.RenderPass
            match passes.TryGetValue key with
                | (true,c) -> 
                    c.Remove o
                | _ ->
                    false

        member x.Clear() =
            for c in passes.Values do
                c.Dispose()
            passes.Clear()
            locks.Clear()
            cmd.Reset()
            x.MarkOutdated()

        override x.Dispose() =
            transact (fun () ->
                x.Clear()
                cmd.Dispose()
                pool.Dispose()
                manager.Dispose()
            )

        override x.FramebufferSignature = Some (renderPass :> _)

        override x.Runtime = None

        override x.PerformUpdate(token : AdaptiveToken, rt : RenderToken) =
            ()

        override x.Use(f : unit -> 'r) =
            f()

        override x.Perform(token : AdaptiveToken, rt : RenderToken, desc : OutputDescription) =
            x.OutOfDate <- true
            let vp = Array.create renderPass.AttachmentCount desc.viewport
            transact (fun () -> viewports.Value <- vp)

            let fbo =
                match desc.framebuffer with
                    | :? Framebuffer as fbo -> fbo
                    | fbo -> failwithf "unsupported framebuffer: %A" fbo

            use tt = device.Token
            let passCmds = passes.Values |> Seq.map (fun p -> p.GetValue(token)) |> Seq.toList
            tt.Sync()

            cmd.Reset()
            cmd.Begin(renderPass, CommandBufferUsage.OneTimeSubmit)
            cmd.enqueue {
                let oldLayouts = Array.zeroCreate fbo.ImageViews.Length
                for i in 0 .. fbo.ImageViews.Length - 1 do
                    let img = fbo.ImageViews.[i].Image
                    oldLayouts.[i] <- img.Layout
                    if VkFormat.hasDepth img.Format then
                        do! Command.TransformLayout(img, VkImageLayout.DepthStencilAttachmentOptimal)
                    else
                        do! Command.TransformLayout(img, VkImageLayout.ColorAttachmentOptimal)

                do! Command.BeginPass(renderPass, fbo, false)
                do! Command.ExecuteSequential passCmds
                do! Command.EndPass

                for i in 0 .. fbo.ImageViews.Length - 1 do
                    let img = fbo.ImageViews.[i].Image
                    do! Command.TransformLayout(img, oldLayouts.[i])
            }   
            cmd.End()

            device.GraphicsFamily.RunSynchronously cmd
            
    type DependentRenderTask(device : Device, renderPass : RenderPass, objects : aset<IRenderObject>, shareTextures : bool, shareBuffers : bool) =
        inherit RenderTask(device, renderPass, shareTextures, shareBuffers)

        let reader = objects.GetReader()

        override x.Perform(token : AdaptiveToken, rt : RenderToken, desc : OutputDescription) =
            x.OutOfDate <- true
            let deltas = reader.GetOperations token
            if not (HDeltaSet.isEmpty deltas) then
                transact (fun () -> 
                    for d in deltas do
                        match d with
                            | Add(_,o) -> x.Add o |> ignore
                            | Rem(_,o) -> x.Remove o |> ignore
                )

            base.Perform(token, rt, desc)

        override x.Dispose() =
            reader.Dispose()
            base.Dispose()




