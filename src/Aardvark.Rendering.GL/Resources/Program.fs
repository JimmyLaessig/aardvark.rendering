﻿namespace Aardvark.Rendering.GL

#nowarn "44" //Obsolete warning

open System
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.InteropServices
open Aardvark.Base
open Aardvark.Base.Incremental
open Aardvark.Base.Rendering
open Aardvark.Base.ShaderReflection
open OpenTK
open OpenTK.Platform
open OpenTK.Graphics
open OpenTK.Graphics.OpenGL4
open Microsoft.FSharp.Quotations


[<AutoOpen>]
module private ShaderProgramCounters =
    let addProgram (ctx : Context) =
        Interlocked.Increment(&ctx.MemoryUsage.ShaderProgramCount) |> ignore

    let removeProgram (ctx : Context) =
        Interlocked.Decrement(&ctx.MemoryUsage.ShaderProgramCount) |> ignore


type ActiveUniform = { slot : int; index : int; location : int; name : string; semantic : string; samplerState : Option<SamplerStateDescription>; size : int; uniformType : ActiveUniformType; offset : int; arrayStride : int; isRowMajor : bool } with
    member x.Interface =

        let name =
            if x.name = x.semantic then x.name
            else sprintf "%s : %s" x.name x.semantic

        match x.samplerState with
            | Some sam ->
                sprintf "%A %s; // sampler: %A" x.uniformType name sam
            | None ->
                sprintf "%A %s;" x.uniformType name
                
type UniformBlock = { name : string; index : int; binding : int; fields : list<ActiveUniform>; size : int; referencedBy : Set<ShaderStage> }
type ActiveAttribute = { attributeIndex : int; size : int; name : string; semantic : string; attributeType : ActiveAttribType }

type Shader =
    class 
        val mutable public Context : Context
        val mutable public Handle : int
        val mutable public Stage : ShaderStage
        val mutable public SupportedModes : Option<Set<IndexedGeometryMode>>
        new(ctx : Context, handle : int, stage : ShaderStage, tops) = { Context = ctx; Handle = handle; Stage = stage; SupportedModes = tops}
    end

[<StructuredFormatDisplay("{InterfaceBlock}")>]
type Program =
    {
       Context : Context
       Code : string
       Handle : int
       Shaders : list<Shader>
       UniformGetters : SymbolDict<IMod>
       SupportedModes : Option<Set<IndexedGeometryMode>>
       Interface : ShaderReflection.ShaderInterface
       TextureInfo : Map<string * int, SamplerDescription>

       [<DefaultValue>]
       mutable _inputs : Option<list<string * Type>>
       [<DefaultValue>]
       mutable _outputs : Option<list<string * Type>>
       [<DefaultValue>]
       mutable _uniforms : Option<list<string * Type>>
    } with

    member x.WritesPointSize =
        x.Interface.UsedBuiltInOutputs |> Map.exists (fun _ s -> s |> Set.contains "gl_PointSize")

    interface IBackendSurface with
        member x.Handle = x.Handle :> obj
        member x.UniformGetters = x.UniformGetters
        member x.Samplers = []

        member x.Inputs = 
            match x._inputs with
                | None -> 
                    let r = x.Interface.Inputs |> List.map (fun a -> ShaderPath.name a.Path, ShaderParameterType.getExpectedType a.Type)
                    x._inputs <- Some r
                    r
                | Some r ->
                    r

        member x.Outputs = 
            match x._outputs with
                | None ->
                    let r = x.Interface.Outputs |> List.map (fun a -> ShaderPath.name a.Path, ShaderParameterType.getExpectedType a.Type)
                    x._outputs <- Some r
                    r
                | Some r ->
                    r

        member x.Uniforms =
            match x._uniforms with
                | None ->
                    let bu = 
                        x.Interface.UniformBlocks |> List.collect (fun b -> 
                            b.Fields |> List.map (fun f -> 
                                ShaderPath.name f.Path, ShaderParameterType.getExpectedType f.Type
                            )
                        )

                    let uu = 
                        x.Interface.Uniforms |> List.map (fun f -> 
                            match f.Type with
                                | Sampler _ -> 
                                    ShaderPath.name f.Path, typeof<ITexture>
                                | t ->
                                    ShaderPath.name f.Path, ShaderParameterType.getExpectedType t
                        )
                    
                    let res = bu @ uu 
                    x._uniforms <- Some res
                    res
                | Some r ->
                    r

[<AutoOpen>]
module ProgramExtensions =
    //type UniformField = Aardvark.Rendering.GL.UniformField
    open System.Text.RegularExpressions
    open System

    let private getShaderType (stage : ShaderStage) =
        match stage with
            | ShaderStage.Vertex -> ShaderType.VertexShader
            | ShaderStage.TessControl -> ShaderType.TessControlShader
            | ShaderStage.TessEval -> ShaderType.TessEvaluationShader
            | ShaderStage.Geometry -> ShaderType.GeometryShader
            | ShaderStage.Fragment -> ShaderType.FragmentShader
            | ShaderStage.Compute -> ShaderType.ComputeShader
            | _ -> failwithf "unknown shader-stage: %A" stage

    let private versionRx = System.Text.RegularExpressions.Regex @"#version[ \t]+(?<version>.*)"
    let private addPreprocessorDefine (define : string) (code : string) =
        let replaced = ref false
        let def = sprintf "#define %s\r\n" define
        let layout = "" //"layout(row_major) uniform;\r\n"

        let newCode = 
            versionRx.Replace(code, System.Text.RegularExpressions.MatchEvaluator(fun m ->
                let v = m.Groups.["version"].Value
                replaced := true
                match Int32.TryParse v with
                    | (true, vers) when vers > 120 ->
                        sprintf "#version %s\r\n%s%s" v def layout
                    | _ ->
                        sprintf "#version %s\r\n%s" v def
            ))

        if !replaced then newCode
        else def + newCode


    let private outputSuffixes = ["{0}"; "{0}Out"; "{0}Frag"; "Pixel{0}"; "{0}Pixel"; "{0}Fragment"]
    let private geometryOutputSuffixes = ["{0}"; "{0}Out"; "{0}Geometry"; "{0}TessControl"; "{0}TessEval"; "Geometry{0}"; "TessControl{0}"; "TessEval{0}"]


    module ShaderCompiler = 
        let tryCompileShader (stage : ShaderStage) (code : string) (entryPoint : string) (x : Context) =
            using x.ResourceLock (fun _ ->
                let code = code.Replace(sprintf "%s(" entryPoint, "main(")
                
                let handle = GL.CreateShader(getShaderType stage)
                GL.Check "could not create shader"

                GL.ShaderSource(handle, code)
                GL.Check "could not attach shader source"

                GL.CompileShader(handle)
                GL.Check "could not compile shader"

                let status = GL.GetShader(handle, ShaderParameter.CompileStatus)
                GL.Check "could not get shader status"

                let log = GL.GetShaderInfoLog handle
                
                let topologies =
                    match stage with
                        | ShaderStage.Geometry ->
                            
                            let inRx = System.Text.RegularExpressions.Regex @"layout\((?<top>[a-zA-Z_]+)\)[ \t]*in[ \t]*;"
                            let m = inRx.Match code
                            if m.Success then
                                match m.Groups.["top"].Value with
                                    | "points" -> 
                                        [IndexedGeometryMode.PointList] |> Set.ofList |> Some

                                    | "lines" ->
                                        [IndexedGeometryMode.LineList; IndexedGeometryMode.LineStrip] |> Set.ofList |> Some

                                    | "lines_adjacency" ->
                                        [IndexedGeometryMode.LineAdjacencyList; IndexedGeometryMode.LineStrip] |> Set.ofList |> Some

                                    | "triangles"  ->
                                        [IndexedGeometryMode.TriangleList; IndexedGeometryMode.TriangleStrip] |> Set.ofList |> Some
                                    
                                    | "triangles_adjacency" ->
                                        [IndexedGeometryMode.TriangleAdjacencyList] |> Set.ofList |> Some
                                    
                                    | v ->
                                       failwithf "unknown geometry shader input topology: %A" v 
                            else
                                failwith "could not determine geometry shader input topology"

                        | _ ->
                            None

                if status = 1 then
                    Success(Shader(x, handle, stage, topologies))
                else
                    let log =
                        if String.IsNullOrEmpty log then "ERROR: shader did not compile but log was empty"
                        else log

                    Error log

            )

        let tryCompileCompute (code : string) (x : Context) =
            use t = x.ResourceLock
            match tryCompileShader ShaderStage.Compute code "main" x with
                | Success shader ->
                    Success [shader]
                | Error err ->
                    Error err

        let withLineNumbers (code : string) : string =
            let lineCount = String.lineCount code
            let lineColumns = 1 + int (Fun.Log10 lineCount)
            let lineFormatLen = lineColumns + 3
            let sb = new System.Text.StringBuilder(code.Length + lineFormatLen * lineCount + 10)
            
            let fmtStr = "{0:" + lineColumns.ToString() + "} : "
            let mutable lineEnd = code.IndexOf('\n')
            let mutable lineStart = 0
            let mutable lineCnt = 1
            while (lineEnd >= 0) do
                let line = code.Substring(lineStart, lineEnd - lineStart + 1)
                sb.Append(lineCnt.ToString().PadLeft(lineColumns)) |> ignore
                sb.Append(": ")  |> ignore
                sb.Append(line) |> ignore
                lineStart <- lineEnd + 1
                lineCnt <- lineCnt + 1
                lineEnd <- code.IndexOf('\n', lineStart)
                ()

            let lastLine = code.Substring(lineStart)
            if lastLine.Length > 0 then
                sb.Append(lineCnt.ToString()) |> ignore
                sb.Append(": ")  |> ignore
                sb.Append(lastLine) |> ignore

            sb.ToString()

        let tryCompileShaders (withFragment : bool) (code : string) (x : Context) =
            let vs = code.Contains "#ifdef Vertex"
            let tcs = code.Contains "#ifdef TessControl"
            let tev = code.Contains "#ifdef TessEval"
            let gs = code.Contains "#ifdef Geometry"
            let fs = withFragment && code.Contains "#ifdef Fragment"

            let stages =
                [
                    if vs then yield "Vertex", "main", ShaderStage.Vertex
                    if tcs then yield "TessControl", "main", ShaderStage.TessControl
                    if tev then yield "TessEval", "main", ShaderStage.TessEval
                    if gs then yield "Geometry", "main", ShaderStage.Geometry
                    if fs then yield "Fragment", "main", ShaderStage.Fragment
                ]

            let code = if (code.Contains("layout(location = 0) out vec4 Colors2Out")) then 
                            code.Replace("out vec4 Colors2Out", "out vec4 Colors3Out")
                                .Replace("out vec4 ColorsOut", "out vec4 Colors2Out")
                                .Replace("out vec4 Colors3Out", "out vec4 ColorsOut")
                       else code

            if RuntimeConfig.PrintShaderCode then
                let codeWithDefine = addPreprocessorDefine "__SHADER_STAGE__" code
                let numberdLines = withLineNumbers codeWithDefine
                Report.Line("Compiling shader:\n{0}", numberdLines)

            using x.ResourceLock (fun _ ->
                let results =
                    stages |> List.map (fun (def, entry, stage) ->
                        let codeWithDefine = addPreprocessorDefine def code
                        stage, tryCompileShader stage codeWithDefine entry x
                    )

                let errors = results |> List.choose (fun (stage,r) -> match r with | Error e -> Some(stage,e) | _ -> None)
                if List.isEmpty errors then
                    let shaders = results |> List.choose (function (_,Success r) -> Some r | _ -> None)
                    Success shaders
                else
                    let codeWithDefine = addPreprocessorDefine "__SHADER_STAGE__" code
                    let numberdLines = withLineNumbers codeWithDefine
                    Report.Line("Failed to compile shader:\n{0}", numberdLines)
                    let err = errors |> List.map (fun (stage, e) -> sprintf "%A:\r\n%s" stage (String.indent 1 e)) |> String.concat "\r\n\r\n" 
                    Error err
            
            )

        let setFragDataLocations (fboSignature : IFramebufferSignature) (handle : int) (x : Context) =
            using x.ResourceLock (fun _ ->
                fboSignature.ColorAttachments 
                    |> Map.toList
                    |> List.map (fun (location, (semantic, signature)) ->
                        let name = semantic.ToString()

                        let outputNameAndIndex = 
                            outputSuffixes |> List.tryPick (fun s ->
                                let outputName = String.Format(s, name)
                                let index = GL.GetFragDataIndex(handle, outputName)
                                GL.Check "could not get FragDataIndex"
                                if index >= 0 then Some (outputName, index)
                                else None
                            )

                            

                        match outputNameAndIndex with
                            | Some (outputName, index) ->
                                GL.BindFragDataLocation(handle, location, semantic.ToString())
                                GL.Check "could not bind FragData location"

                                { attributeIndex = location; size = 1; name = outputName; semantic = semantic.ToString(); attributeType = ActiveAttribType.FloatVec4 }
                            | None ->
                                failwithf "could not get desired program-output: %A" semantic
                    )
            )

        let setTransformFeedbackVaryings (wantedSemantics : list<Symbol>) (p : int) (x : Context) =
            using x.ResourceLock (fun _ ->
                try
                    let outputCount = GL.GetProgramInterface(p, ProgramInterface.ProgramOutput, ProgramInterfaceParameter.ActiveResources)
                    GL.Check "could not get active-output count"

                    let outputs = 
                        [ for i in 0..outputCount-1 do
                            let mutable l = 0
                            let builder = System.Text.StringBuilder(1024)
                            GL.GetProgramResourceName(p, ProgramInterface.ProgramOutput, i, 1024, &l, builder)
                            GL.Check "could not get program resource name"
                            let name = builder.ToString()

                            let mutable prop = ProgramProperty.Type
                            let _,p = GL.GetProgramResource(p, ProgramInterface.ProgramOutput, i, 1, &prop, 1)
                            GL.Check "could not get program resource"
                            let outputType = p |> unbox<ActiveAttribType>

                            let mutable prop = ProgramProperty.ArraySize
                            let _,size = GL.GetProgramResource(p, ProgramInterface.ProgramOutput, i, 1, &prop, 1)
                            GL.Check "could not get program resource"


                            let attrib = { attributeIndex = i; size = 1; name = name; semantic = name; attributeType = outputType }
                            Log.line "found: %A" attrib
                            yield attrib

                        ]   
                    
                    let outputs =
                        let outputMap =
                            outputs 
                                |> List.map (fun o -> o.name, o)
                                |> Map.ofList

                        wantedSemantics |> List.map (fun sem ->
                            let output = 
                                geometryOutputSuffixes |> List.tryPick (fun fmt ->
                                    let name = String.Format(fmt, string sem)
                                    Map.tryFind name outputMap
                                )

                            match output with
                                | Some o -> o
                                | _ -> failwithf "[GL] could not get geometry-output %A" sem

                        )


                    let varyings = 
                        outputs 
                            |> List.map (fun o -> o.name)
                            |> List.toArray

                    GL.TransformFeedbackVaryings(p, varyings.Length, varyings, TransformFeedbackMode.InterleavedAttribs)
                    GL.Check "could not set feedback varyings"

                    outputs

                with e ->
                    []

            )

        let tryLinkProgram (expectsRowMajorMatrices : bool) (handle : int) (code : string) (shaders : list<Shader>) (firstTexture : int) (imageSlots : Map<Symbol, int>) (findOutputs : int -> Context -> list<ActiveAttribute>) (x : Context) =
            GL.LinkProgram(handle)
            GL.Check "could not link program"

         
            let status = GL.GetProgram(handle, GetProgramParameterName.LinkStatus)
            let log = GL.GetProgramInfoLog(handle)
            GL.Check "could not get program log"


            if status = 1 then
                let outputs = findOutputs handle x

                // after modifying the frag-locations the program needs to be linked again
                GL.LinkProgram(handle)
                GL.Check "could not link program"

                let status = GL.GetProgram(handle, GetProgramParameterName.LinkStatus)
                let log = GL.GetProgramInfoLog(handle)
                GL.Check "could not get program log"

                if status = 1 then

                    GL.UseProgram(handle)
                    GL.Check "could not bind program"

                    let supported = 
                        shaders |> List.tryPick (fun s -> s.SupportedModes)

                    let iface = ShaderInterface.ofProgram firstTexture x handle
                    let iface = 
                        if expectsRowMajorMatrices then ShaderInterface.flipMatrixMajority iface
                        else iface

                    GL.UseProgram(0)
                    GL.Check "could not unbind program"


                    Success {
                        Context = x
                        Code = code
                        Handle = handle
                        Shaders = shaders
                        UniformGetters = SymDict.empty
                        SupportedModes = supported
                        Interface = iface
                        TextureInfo = Map.empty
                    }
                else
                    let log =
                        if String.IsNullOrEmpty log then "ERROR: program could not be linked but log was empty"
                        else log

                    Error log

            else
                let log =
                    if String.IsNullOrEmpty log then "ERROR: program could not be linked but log was empty"
                    else log

                Error log

    type Aardvark.Rendering.GL.Context with


        member x.TryCompileShader(stage : ShaderStage, code : string, entryPoint : string) =
            x |> ShaderCompiler.tryCompileShader stage code entryPoint


        member x.TryCompileCompute(expectsRowMajorMatrices : bool, code : string) =
            use t = x.ResourceLock

            match x |> ShaderCompiler.tryCompileCompute code with
                | Success shaders ->
                    addProgram x
                    let handle = GL.CreateProgram()
                    GL.Check "could not create program"

                    for s in shaders do
                        GL.AttachShader(handle, s.Handle)
                        GL.Check "could not attach shader to program"

                    match x |> ShaderCompiler.tryLinkProgram expectsRowMajorMatrices handle code shaders 0 Map.empty (fun _ _ -> []) with
                        | Success program ->
                            Success program
                        | Error err ->
                            Error err

                | Error err ->
                    Error err
                    
        member x.TryCompileProgram(fboSignature : IFramebufferSignature, expectsRowMajorMatrices : bool, code : string) =
            using x.ResourceLock (fun _ ->
                match x |> ShaderCompiler.tryCompileShaders true code with
                    | Success shaders ->

                        let imageSlots = fboSignature.Images |> Map.toSeq |> Seq.map (fun (a,b) -> b,a) |> Map.ofSeq

                        let firstTexture = 
                            if Map.isEmpty fboSignature.Images then
                                0
                            else
                                1 + (fboSignature.Images |> Map.toSeq |> Seq.map fst |> Seq.max)

                        addProgram x
                        let handle = GL.CreateProgram()
                        GL.Check "could not create program"

                        for s in shaders do
                            GL.AttachShader(handle, s.Handle)
                            GL.Check "could not attach shader to program"

                        match x |> ShaderCompiler.tryLinkProgram expectsRowMajorMatrices handle code shaders firstTexture imageSlots (ShaderCompiler.setFragDataLocations fboSignature) with
                            | Success program ->
                                Success program
                            | Error err ->
                                Error err

                    
                    | Error err ->
                        Error err
            )

        member x.TryCompileTransformFeedbackProgram(wantedSemantics : list<Symbol>, expectsRowMajorMatrices : bool, code : string) =
            using x.ResourceLock (fun _ ->
                match x |> ShaderCompiler.tryCompileShaders false code with
                    | Success shaders ->
                        let shaders = shaders |> List.filter (fun s -> s.Stage <> ShaderStage.Fragment)

                        addProgram x
                        let handle = GL.CreateProgram()
                        GL.Check "could not create program"

                        for s in shaders do
                            GL.AttachShader(handle, s.Handle)
                            GL.Check "could not attach shader to program"

             
                        match x |> ShaderCompiler.tryLinkProgram expectsRowMajorMatrices handle code shaders 0 Map.empty (ShaderCompiler.setTransformFeedbackVaryings wantedSemantics) with
                            | Success program ->
                                Success program
                            | Error err ->
                                Error err

                    
                    | Error err ->
                        Error err
            )

        member x.CompileProgram(fboSignature : IFramebufferSignature, expectsRowMajorMatrices : bool, code : string) =
            match x.TryCompileProgram(fboSignature, expectsRowMajorMatrices, code) with
                | Success p -> p
                | Error e ->
                    failwithf "[GL] shader compiler returned errors: %s" e

        member x.CompileTransformFeedbackProgram(wantedSemantics : list<Symbol>, expectsRowMajorMatrices : bool, code : string) =
            match x.TryCompileTransformFeedbackProgram(wantedSemantics, expectsRowMajorMatrices, code) with
                | Success p -> p
                | Error e ->
                    failwithf "[GL] shader compiler returned errors: %s" e

        member x.Delete(p : Program) =
            using x.ResourceLock (fun _ ->
                removeProgram x
                GL.DeleteProgram(p.Handle)
                GL.Check "could not delete program"
            )



