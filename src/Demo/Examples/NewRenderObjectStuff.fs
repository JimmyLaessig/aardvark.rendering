module NewRenderObjectStuff

open Aardvark.Base
open Aardvark.Base.Rendering
open FShade
open Aardvark.Base.Incremental
open Aardvark.Application
open Aardvark.Application.WinForms
open Aardvark.Rendering.Vulkan
open Aardvark.Rendering.Vulkan.RenderCommands
open Aardvark.Rendering.Vulkan.RenderTaskNew
open Aardvark.SceneGraph


type LazyTree<'a> =
    | Empty
    | Leaf of Lazy<'a>
    | Node of Option<Lazy<'a>> * list<Lazy<LazyTree<'a>>>

let run() =
    use app = new VulkanApplication(true)
    let win = app.CreateSimpleRenderWindow()

    let pass = win.FramebufferSignature |> unbox<RenderPass>
    let device = app.Runtime.Device

    let task = new RenderTask(device, pass, true, true)

    let effect =
        Effect.compose [
            DefaultSurfaces.trafo |> toEffect
            DefaultSurfaces.constantColor C4f.Red |> toEffect
            DefaultSurfaces.simpleLighting |> toEffect
        ]

    let cam = CameraView.lookAt (V3d(2,2,2)) V3d.Zero V3d.OOI
    let cam = cam |> DefaultCameraController.control win.Mouse win.Keyboard win.Time
    let frustum = win.Sizes |> Mod.map (fun s -> Frustum.perspective 60.0 0.1 100.0 (float s.X / float s.Y))

    let pipeline =
        {
            surface             = Aardvark.Base.Surface.FShadeSimple effect

            depthTest           = Mod.constant DepthTestMode.LessOrEqual
            cullMode            = Mod.constant CullMode.None
            blendMode           = Mod.constant BlendMode.None
            fillMode            = Mod.constant FillMode.Fill
            stencilMode         = Mod.constant StencilMode.Disabled
            multisample         = Mod.constant true
            writeBuffers        = None
            globalUniforms      = UniformProvider.Empty
            geometryMode        = IndexedGeometryMode.TriangleList
            vertexInputTypes    = Map.ofList [ DefaultSemantic.Positions, typeof<V3f>; DefaultSemantic.Normals, typeof<V3f> ]
            perGeometryUniforms = Map.empty
        } 
        
    let viewTrafo = cam |> Mod.map CameraView.viewTrafo
    let projTrafo = frustum |> Mod.map Frustum.projTrafo

    let uniforms =
        Map.ofList [
            "ViewTrafo", viewTrafo :> IMod
            "ProjTrafo", projTrafo :> IMod
            "LightLocation", viewTrafo |> Mod.map (fun (t : Trafo3d) -> t.GetViewPosition()) :> IMod
        ]

    let mkGeometry (scale : float) (pos : V3d) =
        
        let sphere = Primitives.unitSphere 6
        let positions = sphere.IndexedAttributes.[DefaultSemantic.Positions] |> unbox<V3f[]>
        let normals = sphere.IndexedAttributes.[DefaultSemantic.Positions] |> unbox<V3f[]>
        let buf a = a |> ArrayBuffer :> IBuffer |> Mod.constant

        {
            vertexAttributes    = Map.ofList [ DefaultSemantic.Positions, buf positions; DefaultSemantic.Normals, buf normals ]
            indices             = None
            uniforms            = uniforms |> Map.add "ModelTrafo" (Mod.constant (Trafo3d.Scale scale * Trafo3d.Translation pos) :> IMod)
            call                = Mod.constant [ DrawCallInfo(FaceVertexCount = positions.Length, InstanceCount = 1) ]
        }

    let thing = Tree.Leaf (mkGeometry 1.0 V3d.Zero)

    
    let rec build (center : V3d) (scale : float) =
        lazy (
            let children =
                [
                    center + V3d(-1,-1,-1) * scale / 2.0
                    center + V3d(-1,-1,1) * scale / 2.0
                    center + V3d(-1,1,-1) * scale / 2.0
                    center + V3d(-1,1,1) * scale / 2.0
                    center + V3d(1,-1,-1) * scale / 2.0
                    center + V3d(1,-1,1) * scale / 2.0
                    center + V3d(1,1,-1) * scale / 2.0
                    center + V3d(1,1,1) * scale / 2.0
                ]
            LazyTree.Node(Some (lazy (mkGeometry scale center)), children |> List.map (fun o -> build o (scale / 2.0)))
        )

    let infTree =
        build V3d.Zero 2.0

    let rec traverse (viewProj : Trafo3d) =
        Tree.Leaf(mkGeometry 1.0 V3d.Zero)

    let tree = 
        Mod.map2 (*) viewTrafo projTrafo
            |> Mod.map (fun vp -> traverse vp) 

    let o = TreeRenderObject(pipeline, tree)


    task.Add(o) |> ignore


    win.RenderTask <- task



    win.Run()
    win.Dispose()



