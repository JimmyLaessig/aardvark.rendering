﻿namespace Aardvark.Rendering.Vulkan

open System
open System.Threading
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open Aardvark.Base
open Aardvark.Base.Rendering
open Aardvark.Rendering.Vulkan
open Microsoft.FSharp.NativeInterop
open Aardvark.Base.Incremental

#nowarn "9"
#nowarn "51"

type PreparedRenderObject =
    {
        device                  : Device
        original                : RenderObject
        program                 : IResource<ShaderProgram>
        descriptorResources     : list<IResource>
        descriptorSets          : list<IResource<DescriptorSet>>
        pipeline                : IResource<Pipeline>
        vertexBuffers           : array<IResource<Buffer> * int>
        indexBuffer             : Option<IResource<Buffer>>
        activation              : IDisposable
    }
    member x.DrawCallInfos = x.original.DrawCallInfos
    member x.RenderPass = x.original.RenderPass
    member x.AttributeScope = x.original.AttributeScope

    member x.Dispose() =
        //x.geometryMode.Dispose()
        x.activation.Dispose()
        x.program.Dispose()
        x.pipeline.Dispose()
        for r in x.descriptorResources do r.Dispose()
        for r in x.descriptorSets do r.Dispose()

        for (b,_) in x.vertexBuffers do b.Dispose()


        match x.indexBuffer with
            | Some ib -> ib.Dispose()
            | None -> ()

    member x.Update(caller : IAdaptiveObject) =
        use token = x.device.ResourceToken
        let mutable stats = FrameStatistics.Zero
        stats <- stats + x.pipeline.Update(caller)

        for r in x.descriptorSets do stats <- stats + r.Update(caller)
        for (b,_) in x.vertexBuffers do stats <- stats + b.Update(caller)

//        match x.indirect with
//            | Some b -> do! b.Update(caller)
//            | None -> ()

        match x.indexBuffer with
            | Some b -> stats <- stats + b.Update(caller)
            | None -> ()

        stats

    member x.IncrementReferenceCount() =
        //x.geometryMode.IncrementReferenceCount()
        x.program.AddRef()
        x.pipeline.AddRef()

        for r in x.descriptorResources do r.AddRef()
        for r in x.descriptorSets do r.AddRef()
        for (b,_) in x.vertexBuffers do b.AddRef()

//        match x.indirect with
//            | Some ib -> ib.AddRef()
//            | None -> ()

        match x.indexBuffer with
            | Some ib -> ib.AddRef()
            | None -> ()

    interface IPreparedRenderObject with
        member x.Id = x.original.Id
        member x.RenderPass = x.original.RenderPass
        member x.AttributeScope = x.original.AttributeScope
        member x.Update(caller) = x.Update(caller) |> ignore
        member x.Original = Some x.original
        member x.Dispose() = x.Dispose()

[<AbstractClass; Sealed; Extension>]
type DevicePreparedRenderObjectExtensions private() =
    
    [<Extension>]
    static member PrepareRenderObject(this : ResourceManager, renderPass : RenderPass, ro : IRenderObject) =
        match ro with
            | :? RenderObject as ro ->
                let program = this.CreateShaderProgram(renderPass, ro.Surface)
                let prog = program.Handle.GetValue()

                let mutable descriptorResources : list<IResource> = []

                let descriptorSets = 
                    prog.PipelineLayout.DescriptorSetLayouts |> List.map (fun ds ->
                        let bufferBindings, imageBindings = 
                            ds.Bindings |> List.partition (fun b ->
                                match b.Parameter.paramType with
                                    | ShaderType.Ptr(_, ShaderType.Struct _) -> true
                                    | _ -> false
                            )
                    
                        let buffers =
                            bufferBindings |> List.map (fun b ->
                                match b.Parameter.paramType with
                                    | ShaderType.Ptr(_, ShaderType.Struct(_,fields)) ->
                                        let layout = UniformBufferLayoutStd140.structLayout fields
                                        let buffer = this.CreateUniformBuffer(ro.AttributeScope, layout, ro.Uniforms, prog.UniformGetters)
                                        descriptorResources <- (buffer :> _) :: descriptorResources
                                        b.Binding, buffer
                                    | _ ->
                                        failf "impossible"
                            )

                        let textures =
                            imageBindings |> List.map (fun desc ->
                                 match desc.Parameter.paramType with
                                    | ShaderType.Ptr(_, ShaderType.Image(sampledType, dim, isDepth, isArray, isMS, _, _)) 
                                    | ShaderType.Image(sampledType, dim, isDepth, isArray, isMS, _, _) ->
                                        let sym = Symbol.Create desc.Parameter.paramName

                                        let sym =
                                            match prog.Surface.SemanticMap.TryGetValue sym with
                                                | (true, sem) -> sem
                                                | _ -> sym

                                        let binding = desc.Binding

                                        let samplerState = 
                                            match prog.Surface.SamplerStates.TryGetValue sym with
                                                | (true, sam) -> sam
                                                | _ -> 
                                                    Log.warn "could not get sampler for texture: %A" sym
                                                    SamplerStateDescription()



                                        match ro.Uniforms.TryGetUniform(Ag.emptyScope, sym) with
                                            | Some (:? IMod<ITexture> as tex) ->

                                                let tex = this.CreateImage(tex)
                                                let view = this.CreateImageView(tex)
                                                let sam = this.CreateSampler(Mod.constant samplerState)
                                                
                                                descriptorResources <- (tex :> _) :: (sam :> _) :: (view :> _) :: descriptorResources
                                                binding, (view, sam)
                                            | _ -> 
                                                failwithf "could not find texture: %A" desc.Parameter
                                    | _ ->
                                        failf "bad uniform-type %A" desc
                            )
                    
                        this.CreateDescriptorSet(ds, Map.ofList buffers, Map.ofList textures)
                    )


                let isCompatible (shaderType : ShaderType) (dataType : Type) =
                    // TODO: verify type compatibility
                    true

                let bufferViews =
                    let vs = prog.Shaders.[ShaderStage.Vertex]

                    vs.Interface.inputs
                        |> Seq.map (fun param ->
                            match ShaderParameter.tryGetLocation param with
                                | Some loc -> loc, param.paramName, param.paramType
                                | None -> failwithf "no attribute location given for shader input: %A" param
                            )

                        |> Seq.map (fun (location, parameterName, parameterType) ->
                            let sym = Symbol.Create parameterName

                            let perInstance, view =
                                match ro.VertexAttributes.TryGetAttribute sym with
                                    | Some att -> false, att
                                    | None ->
                                        match ro.InstanceAttributes.TryGetAttribute sym with
                                            | Some att -> true, att
                                            | None -> failwithf "could not get vertex data for shader input: %A" parameterName

                            if isCompatible parameterType view.ElementType then
                                (parameterName, location, perInstance, view)
                            else
                                failwithf "vertex data has incompatible type for shader: %A vs %A" view.ElementType parameterType
                            )
                        |> Seq.toList

                let buffers =
                    bufferViews 
                        |> Seq.sortBy (fun (_,l,_,_) -> l)
                        |> Seq.map (fun (name,loc, _, view) ->
                            let buffer = this.CreateBuffer(view.Buffer)
                            buffer, view.Offset
                            )
                        |> Seq.toArray


                let bufferFormats = 
                    bufferViews |> Seq.map (fun (name,location, perInstance, view) -> name, (perInstance, view.ElementType)) |> Map.ofSeq


                let pipeline =
                    this.CreatePipeline(
                        renderPass, program,
                        bufferFormats,
                        ro.Mode,
                        ro.FillMode, 
                        ro.CullMode,
                        ro.BlendMode,
                        ro.DepthTest,
                        ro.StencilMode
                    )

                let indexBuffer =
                    match ro.Indices with
                        | Some view -> this.CreateIndexBuffer(view.Buffer) |> Some
                        | None -> None

                let res = 
                    {
                        device                      = this.Device
                        original                    = ro
                        program                     = this.CreateShaderProgram(renderPass, ro.Surface)
                        descriptorResources         = descriptorResources
                        descriptorSets              = descriptorSets
                        pipeline                    = pipeline
                        vertexBuffers               = buffers
                        indexBuffer                 = indexBuffer
                        activation                  = ro.Activate()
                    }

                res.Update(null) |> ignore

                res

            | _ ->
                failf "unsupported RenderObject-type: %A" ro
