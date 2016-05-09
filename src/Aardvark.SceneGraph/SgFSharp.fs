﻿namespace Aardvark.SceneGraph


open System
open Aardvark.Base
open Aardvark.Base.Rendering
open Aardvark.Base.Ag
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Runtime.CompilerServices

open Aardvark.Base.Incremental

#nowarn "9"
#nowarn "51"

[<AutoOpen>]
module SgFSharp =

    module Sg =

        let uniform (name : string) (value : IMod<'a>) (sg : ISg) =
            Sg.UniformApplicator(name, value :> IMod, sg) :> ISg

        let trafo (m : IMod<Trafo3d>) (sg : ISg) =
            Sg.TrafoApplicator(m, sg) :> ISg

        let viewTrafo (m : IMod<Trafo3d>) (sg : ISg) =
            Sg.ViewTrafoApplicator(m, sg) :> ISg

        let projTrafo (m : IMod<Trafo3d>) (sg : ISg) =
            Sg.ProjectionTrafoApplicator(m, sg) :> ISg

        let scale (s : float) (sg : ISg) =
            sg |> trafo (s |> Trafo3d.Scale |> Mod.constant)

        let translate (x : float) (y : float) (z : float) (sg : ISg) =
            sg |> trafo (Trafo3d.Translation(x,y,z) |> Mod.constant)

        let transform (t : Trafo3d) (sg : ISg) =
            sg |> trafo (t |> Mod.constant)



        let camera (cam : IMod<Camera>) (sg : ISg) =
            sg |> viewTrafo (cam |> Mod.map Camera.viewTrafo) |> projTrafo (cam |> Mod.map Camera.projTrafo)

        let surface (m : IMod<ISurface>) (sg : ISg) =
            Sg.SurfaceApplicator(m, sg) :> ISg

        let group (s : #seq<ISg>) =
            Sg.Group s

        let group' (s : #seq<ISg>) =
            Sg.Group s :> ISg


        let set (set : aset<ISg>) =
            Sg.Set(set) :> ISg

        let ofSeq (s : seq<#ISg>) =
            s |> Seq.cast<ISg> |> ASet.ofSeq |> Sg.Set :> ISg

        let ofList (l : list<#ISg>) =
            l |> ofSeq

        let ofArray (arr : array<#ISg>) =
            arr |> ofSeq



        let andAlso (sg : ISg) (andSg : ISg) = 
            group [sg;andSg] :> ISg

        let geometrySet mode attributeTypes (geometries : aset<_>) =
            Sg.GeometrySet(geometries,mode,attributeTypes) :> ISg

        let dynamic (s : IMod<ISg>) = 
            Sg.DynamicNode(s) :> ISg

        let onOff (active : IMod<bool>) (sg : ISg) =
            Sg.OnOffNode(active, sg) :> ISg

        let texture (sem : Symbol) (tex : IMod<ITexture>) (sg : ISg) =
            Sg.TextureApplicator(sem, tex, sg) :> ISg

        let diffuseTexture (tex : IMod<ITexture>) (sg : ISg) = 
            texture DefaultSemantic.DiffuseColorTexture tex sg

        let diffuseTexture' (tex : ITexture) (sg : ISg) = 
            texture DefaultSemantic.DiffuseColorTexture (Mod.constant tex) sg

        let diffuseFileTexture' (path : string) (wantMipMaps : bool) (sg : ISg) = 
            texture DefaultSemantic.DiffuseColorTexture (Mod.constant (FileTexture(path, wantMipMaps) :> ITexture)) sg


        let scopeDependentTexture (sem : Symbol) (tex : Scope -> IMod<ITexture>) (sg : ISg) =
            Sg.UniformApplicator(new Providers.ScopeDependentUniformHolder([sem, fun s -> tex s :> IMod]), sg) :> ISg

        let scopeDependentDiffuseTexture (tex : Scope -> IMod<ITexture>) (sg : ISg) =
            scopeDependentTexture DefaultSemantic.DiffuseColorTexture tex sg

        let runtimeDependentTexture (sem : Symbol) (tex : IRuntime -> IMod<ITexture>) (sg : ISg) =
            let cache = Dictionary<IRuntime, IMod<ITexture>>()
            let tex runtime =
                match cache.TryGetValue runtime with
                    | (true, v) -> v
                    | _ -> 
                        let v = tex runtime
                        cache.[runtime] <- v
                        v

            scopeDependentTexture sem (fun s -> s?Runtime |> tex) sg

        let runtimeDependentDiffuseTexture(tex : IRuntime -> IMod<ITexture>) (sg : ISg) =
            runtimeDependentTexture DefaultSemantic.DiffuseColorTexture tex sg

        let fillMode (m : IMod<FillMode>) (sg : ISg) =
            Sg.FillModeApplicator(m, sg) :> ISg
        
        let blendMode (m : IMod<BlendMode>) (sg : ISg) =
            Sg.BlendModeApplicator(m, sg) :> ISg

        let cullMode (m : IMod<CullMode>) (sg : ISg) =
            Sg.CullModeApplicator(m, sg) :> ISg

        let depthTest (m : IMod<DepthTestMode>) (sg : ISg) =
            Sg.DepthTestModeApplicator(m, sg) :> ISg

        let private arrayModCache = ConditionalWeakTable<IMod, IMod<Array>>()

        let private modOfArray (m : IMod<'a[]>) =
            match arrayModCache.TryGetValue (m :> IMod) with
                | (true, r) -> r
                | _ -> 
                    let r = m |> Mod.map (fun a -> a :> Array)
                    arrayModCache.Add(m, r)
                    r

        let vertexAttribute<'a when 'a : struct> (s : Symbol) (value : IMod<'a[]>) (sg : ISg) =
            let view = BufferView(value |> Mod.map (fun data -> ArrayBuffer(data) :> IBuffer), typeof<'a>)
            Sg.VertexAttributeApplicator(Map.ofList [s, view], Mod.constant sg) :> ISg

        let index<'a when 'a : struct> (value : IMod<'a[]>) (sg : ISg) =
            Sg.VertexIndexApplicator(modOfArray value, sg) :> ISg

        let vertexAttribute'<'a when 'a : struct> (s : Symbol) (value : 'a[]) (sg : ISg) =
            let view = BufferView(Mod.constant (ArrayBuffer(value :> Array) :> IBuffer), typeof<'a>)
            Sg.VertexAttributeApplicator(Map.ofList [s, view], Mod.constant sg) :> ISg

        let index'<'a when 'a : struct> (value : 'a[]) (sg : ISg) =
            Sg.VertexIndexApplicator(Mod.constant (value :> Array), sg) :> ISg

        let vertexBuffer (s : Symbol) (view : BufferView) (sg : ISg) =
            Sg.VertexAttributeApplicator(s, view, sg) :> ISg

        let vertexBufferValue (s : Symbol) (value : IMod<V4f>) (sg : ISg) =
            let view = BufferView(value |> Mod.map (fun v -> NullBuffer(v) :> IBuffer), typeof<V4f>)
            Sg.VertexAttributeApplicator(s, view, sg) :> ISg

        let draw (mode : IndexedGeometryMode) =
            Sg.RenderNode(
                DrawCallInfo(
                    FirstInstance = 0,
                    InstanceCount = 1,
                    FirstIndex = 0,
                    FaceVertexCount = -1,
                    BaseVertex = 0
                ),
                mode
            ) :> ISg

        let render (mode : IndexedGeometryMode) (call : DrawCallInfo) =
            Sg.RenderNode(call,mode)

        let ofIndexedGeometry (g : IndexedGeometry) =
            let attributes = 
                g.IndexedAttributes |> Seq.map (fun (KeyValue(k,v)) -> 
                    let t = v.GetType().GetElementType()
                    let view = BufferView(Mod.constant (ArrayBuffer(v) :> IBuffer), t)

                    k, view
                ) |> Map.ofSeq
        

            let index, faceVertexCount =
                if g.IsIndexed then
                    g.IndexArray, g.IndexArray.Length
                else
                    null, g.IndexedAttributes.[DefaultSemantic.Positions].Length

            let call = 
                DrawCallInfo(
                    FaceVertexCount = faceVertexCount,
                    FirstIndex = 0,
                    InstanceCount = 1,
                    FirstInstance = 0,
                    BaseVertex = 0
                )

            let sg = Sg.VertexAttributeApplicator(attributes, Sg.RenderNode(call,g.Mode)) :> ISg
            if index <> null then
                Sg.VertexIndexApplicator(Mod.constant index, sg) :> ISg
            else
                sg

        module private Interleaved = 
            open System.Reflection
            open Microsoft.FSharp.NativeInterop

            type Converter() =
                
                static let toFloat32 (i : 'a) : float32 =
                    let mutable i = i
                    NativePtr.read (&&i |> NativePtr.toNativeInt |> NativePtr.ofNativeInt)

                static let accessors =
                    Dict.ofList [
                        typeof<int8>,    (1, (fun (v : int8)    -> [|toFloat32 (int32 v)|]) :> obj)
                        typeof<uint8>,   (1, (fun (v : uint8)   -> [|toFloat32 (uint32 v)|]) :> obj)
                        typeof<int16>,   (1, (fun (v : int16)   -> [|toFloat32 (int32 v)|]) :> obj)
                        typeof<uint16>,  (1, (fun (v : uint16)  -> [|toFloat32 (uint32 v)|]) :> obj)
                        typeof<int32>,   (1, (fun (v : int32)   -> [|toFloat32 v|]) :> obj)
                        typeof<uint32>,  (1, (fun (v : uint32)  -> [|toFloat32 v|]) :> obj)

                        typeof<float>,   (1, (fun (v : float) -> [|float32 v|]) :> obj)
                        typeof<V2d>,     (2, (fun (v : V2d) -> [|float32 v.X; float32 v.Y|]) :> obj)
                        typeof<V3d>,     (3, (fun (v : V3d) -> [|float32 v.X; float32 v.Y; float32 v.Z|]) :> obj)
                        typeof<V4d>,     (4, (fun (v : V4d) -> [|float32 v.X; float32 v.Y; float32 v.Z; float32 v.W|]) :> obj)
                        
                        typeof<float32>, (1, (fun (v : float32) -> [|v|]) :> obj)
                        typeof<V2f>,     (2, (fun (v : V2f) -> [|v.X; v.Y|]) :> obj)
                        typeof<V3f>,     (3, (fun (v : V3f) -> [|v.X; v.Y; v.Z|]) :> obj)
                        typeof<V4f>,     (4, (fun (v : V4f) -> [|v.X; v.Y; v.Z; v.W|]) :> obj)
                        
                        typeof<C3f>,     (3, (fun (c : C3f) -> [|c.R; c.G; c.B|]) :> obj)
                        typeof<C4f>,     (4, (fun (c : C4f) -> [|c.R; c.G; c.B; c.A|]) :> obj)
                        typeof<C3b>,     (3, (fun (v : C3b) -> let c = v.ToC3f() in [|c.R; c.G; c.B|]) :> obj)
                        typeof<C4b>,     (4, (fun (v : C4b) -> let c = v.ToC4f() in [|c.R; c.G; c.B; c.A|]) :> obj)
                    ]

                static member ToFloatArray (arr : 'a[]) : float32[] =
                    match accessors.TryGetValue typeof<'a> with
                        | (true, (_,accessor)) ->
                            arr |> Array.collect (unbox accessor)
                        | _ -> failwithf "unsupported attribute type: %A" typeof<'a>.FullName
                            

                static member GetDimension (t : Type) =
                    match accessors.TryGetValue t with
                        | (true, (d, arr)) -> d
                        | _ -> failwithf "unsupported attribute type: %A" t.FullName

            let toFloatArrayMeth = typeof<Converter>.GetMethod("ToFloatArray", BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic)

            let toFloatArray (arr : Array) =
                let t = arr.GetType().GetElementType()
                let mi = toFloatArrayMeth.MakeGenericMethod [|t|]
                let arr = mi.Invoke(null, [|arr|]) |> unbox<float32[]>

                Converter.GetDimension(t), arr


        let ofIndexedGeometryInterleaved (attributes : list<Symbol>) (g : IndexedGeometry) =
            let arrays =
                attributes |> List.choose (fun att ->
                    match g.IndexedAttributes.TryGetValue att with
                        | (true, v) -> 
                            let (dim, arr) = Interleaved.toFloatArray v
                            Some (att, v.GetType().GetElementType(), dim, arr)
                        | _ -> None
                )

            let count = arrays |> List.map (fun (_,_,dim,arr) -> arr.Length / dim) |> List.min
            let vertexSize = arrays |> List.map (fun (_,_,d,_) -> d) |> List.sum

            let views = SymbolDict()
            let target = Array.zeroCreate (count * vertexSize)
            let buffer = Mod.constant (ArrayBuffer target :> IBuffer)
            let mutable current = 0
            for vi in 0..count-1 do
                for (sem, t, size, a) in arrays do
                    let mutable start = size * vi
                    for c in 0..size-1 do
                        target.[current] <- a.[start]
                        current <- current + 1
                        start <- start + 1

            let mutable offset = 0
            for (sem, t, size, a) in arrays do
                let v = BufferView(buffer, t, offset * sizeof<float32>, vertexSize * sizeof<float32>)
                views.[sem] <- v
                offset <- offset + size



            let index, faceVertexCount =
                if g.IsIndexed then
                    g.IndexArray, g.IndexArray.Length
                else
                    null, count

            let call = 
                DrawCallInfo(
                    FaceVertexCount = faceVertexCount,
                    FirstIndex = 0,
                    InstanceCount = 1,
                    FirstInstance = 0,
                    BaseVertex = 0
                )

            let sg = Sg.VertexAttributeApplicator(views, Sg.RenderNode(call,g.Mode)) :> ISg
            if index <> null then
                Sg.VertexIndexApplicator(Mod.constant index, sg) :> ISg
            else
                sg

        let instancedGeometry (trafos : IMod<Trafo3d[]>) (g : IndexedGeometry) =
            let vertexAttributes = 
                g.IndexedAttributes |> Seq.map (fun (KeyValue(k,v)) -> 
                    let t = v.GetType().GetElementType()
                    let view = BufferView(Mod.constant (ArrayBuffer(v) :> IBuffer), t)

                    k, view
                ) |> Map.ofSeq

            let index, faceVertexCount =
                if g.IsIndexed then
                    g.IndexArray, g.IndexArray.Length
                else
                    null, g.IndexedAttributes.[DefaultSemantic.Positions].Length

            let call = trafos |> Mod.map (fun t ->
                    DrawCallInfo(
                        FaceVertexCount = faceVertexCount,
                        FirstIndex = 0,
                        InstanceCount = t.Length,
                        FirstInstance = 0,
                        BaseVertex = 0
                    )
                )
            let m44Trafos = trafos |> Mod.map (fun a -> a |> Array.map (fun (t : Trafo3d) -> (M44f.op_Explicit t.Forward).Transposed) :> Array)
            let ibuffer = m44Trafos |> Mod.map (fun a -> ArrayBuffer a :> IBuffer)

            let call = Mod.map2 (fun c _ -> c) call ibuffer
            let m44Bla = Mod.map2 (fun _ m -> m) call ibuffer

            let m44View = BufferView(m44Bla, typeof<M44f>)
            let sg = Sg.VertexAttributeApplicator(vertexAttributes, Sg.RenderNode(call, Mod.constant g.Mode)) :> ISg
        
            let sg =
                if index <> null then
                    Sg.VertexIndexApplicator(Mod.constant index, sg) :> ISg
                else
                    sg


            Sg.InstanceAttributeApplicator([DefaultSemantic.InstanceTrafo, m44View] |> Map.ofList, sg) :> ISg

        let pass (pass : RenderPass) (sg : ISg) = Sg.PassApplicator(pass, sg) :> ISg

        let normalizeToAdaptive (box : Box3d) (this : ISg) =

            let getBoxScale (fromBox : Box3d) (toBox : Box3d) : float =
                let fromSize = fromBox.Size
                let toSize = toBox.Size
                let factor = toSize / fromSize

                let mutable smallest = factor.X

                if factor.Y < smallest then
                    smallest <- factor.Y
                if factor.Z < smallest then
                    smallest <- factor.Z

                smallest

            let bb = this?GlobalBoundingBox() : IMod<Box3d>

            printfn "normalizing from: %A" ( bb.GetValue() )

            let transformBox (sbox : Box3d) = Trafo3d.Translation(-sbox.Center) * Trafo3d.Scale(getBoxScale sbox box) * Trafo3d.Translation(box.Center)

            Sg.TrafoApplicator(Mod.map transformBox bb, this) :> ISg

        let normalizeTo (box : Box3d) (this : ISg) =

            let getBoxScale (fromBox : Box3d) (toBox : Box3d) : float =
                let fromSize = fromBox.Size
                let toSize = toBox.Size
                let factor = toSize / fromSize

                let mutable smallest = factor.X

                if factor.Y < smallest then
                    smallest <- factor.Y
                if factor.Z < smallest then
                    smallest <- factor.Z

                smallest

            let bb = this?GlobalBoundingBox() : IMod<Box3d>

            let transformBox (sbox : Box3d) = Trafo3d.Translation(-sbox.Center) * Trafo3d.Scale(getBoxScale sbox box) * Trafo3d.Translation(box.Center)

            Sg.TrafoApplicator(bb.GetValue() |> transformBox |> Mod.constant, this) :> ISg

        let normalizeAdaptive sg = sg |> normalizeToAdaptive ( Box3d( V3d(-1,-1,-1), V3d(1,1,1) ) ) 
        
        let normalize sg = sg |> normalizeTo ( Box3d( V3d(-1,-1,-1), V3d(1,1,1) ) ) 

        let loadAsync (fboSignature : IFramebufferSignature) (sg : ISg) = Sg.AsyncLoadApplicator(fboSignature, Mod.constant sg) :> ISg

        let overlay (task : IRenderTask) =
            Sg.OverlayNode(task) :> ISg


    type IndexedGeometry with
        member x.Sg =
            Sg.ofIndexedGeometry x
