﻿namespace Aardvark.Rendering.GL.Compiler

#nowarn "9"

open Microsoft.FSharp.NativeInterop
open Aardvark.Base.Incremental
open Aardvark.Base
open Aardvark.Base.Runtime
open Aardvark.Base.Rendering
open Aardvark.Rendering.GL

module DeltaCompiler =


    /// determines if all uniforms (given in values) are equal to the uniforms
    /// of the given RenderObject
    let rec private allUniformsEqual (rj : RenderObject) (values : list<string * IMod>) =
        match values with
            | (k,v)::xs -> 
                match rj.Uniforms.TryGetUniform(rj.AttributeScope, Symbol.Create k) with
                    | Some c ->
                        if c = v then
                            allUniformsEqual rj xs
                        else
                            false

                    | _ -> false

            | [] -> true

    let compileDelta (prev : PreparedRenderObject) (me : PreparedRenderObject) =
        compiled {

            // set the output-buffers
            if prev.DepthBufferMask <> me.DepthBufferMask then
                yield Instructions.setDepthMask me.DepthBufferMask

            if prev.StencilBufferMask <> me.StencilBufferMask then
                yield Instructions.setStencilMask me.StencilBufferMask

            if prev.DrawBuffers <> me.DrawBuffers then
                match me.DrawBuffers with
                    | None -> 
                        let! s = compilerState
                        yield Instruction.DrawBuffers s.info.drawBufferCount s.info.drawBuffers
                    | Some b ->
                        yield Instruction.DrawBuffers b.Count (NativePtr.toNativeInt b.Buffers)

            //set all modes if needed
            if prev.DepthTest <> me.DepthTest && me.DepthTest <> null then
                yield Instructions.setDepthTest me.DepthTest

            if prev.FillMode <> me.FillMode && me.FillMode <> null then
                yield Instructions.setFillMode me.FillMode

            if prev.CullMode <> me.CullMode && me.CullMode <> null then
                yield Instructions.setCullMode me.CullMode

            if prev.BlendMode <> me.BlendMode && me.BlendMode <> null then
                yield Instructions.setBlendMode me.BlendMode

            if prev.StencilMode <> me.StencilMode && me.StencilMode <> null then
                yield Instructions.setStencilMode me.StencilMode

            // bind the program (if needed)
            if prev.Program <> me.Program then
                yield Instructions.bindProgram me.Program

            // bind all uniform-buffers (if needed)
            for (id,ub) in Map.toSeq me.UniformBuffers do
                do! useUniformBufferSlot id

                match Map.tryFind id prev.UniformBuffers with
                    | Some old when old = ub -> 
                        // the same UniformBuffer has already been bound
                        ()
                    | _ -> 
                        yield Instructions.bindUniformBufferView id ub

            // bind all textures/samplers (if needed)
            let latestSlot = ref prev.LastTextureSlot
            for (id,(tex,sam)) in Map.toSeq me.Textures do
                do! useTextureSlot id

                let texEqual, samEqual =
                    match Map.tryFind id prev.Textures with
                        | Some (ot, os) -> (ot = tex), (os = sam)
                        | _ -> false, false


                if id <> !latestSlot then
                    yield Instructions.setActiveTexture id
                    latestSlot := id 

                if not texEqual then 
                    yield Instructions.bindTexture tex

                if not samEqual || (not ExecutionContext.samplersSupported && not texEqual) then
                    yield Instructions.bindSampler id sam

            // bind all top-level uniforms (if needed)
            for (id,u) in Map.toSeq me.Uniforms do
                match Map.tryFind id prev.Uniforms with
                    | Some old when old = u -> ()
                    | _ ->
                        // TODO: UniformLocations cannot change structurally atm.
                        yield ExecutionContext.bindUniformLocation id (u.Handle.GetValue())


            // bind the VAO (if needed)
            if prev.VertexArray <> me.VertexArray then
                yield Instructions.bindVertexArray me.VertexArray

            // bind vertex attribute default values
            for (id,v) in Map.toSeq me.VertexAttributeValues do
                match Map.tryFind id prev.VertexAttributeValues with
                    | Some ov when v = ov -> ()
                    | _ -> 
                        yield Instructions.bindVertexAttribValue id v

            // draw the thing
            // TODO: surface assumed to be constant here
            let prog = me.Program.Handle.GetValue()

            match me.IndirectBuffer with
                | Some ib ->
                    yield Instructions.bindIndirectBuffer ib
                    yield Instructions.drawIndirect prog me.Original.Indices ib me.Mode me.IsActive
                | _ ->
                    yield Instructions.draw prog me.Original.Indices me.DrawCallInfos.Handle me.Mode me.IsActive

        }   

    let compileFull (me : PreparedRenderObject) =
        compileDelta PreparedRenderObject.empty me

    let compileEpilog (prev : Option<PreparedMultiRenderObject>) =
        compiled {
            let! s = compilerState
            let textures = s.info.structuralChange |> Mod.map (fun () -> !s.info.usedTextureSlots)
            let ubos = s.info.structuralChange |> Mod.map (fun () -> !s.info.usedUniformBufferSlots)

            match prev with
                | Some prev ->
                    if not prev.Last.DepthBufferMask then
                        yield Instruction.DepthMask 1

                    if not prev.Last.StencilBufferMask then
                        yield Instruction.StencilMask 0xFFFFFFFF

                    if Option.isSome prev.Last.DrawBuffers then
                        let! s = compilerState
                        yield Instruction.DrawBuffers s.info.drawBufferCount s.info.drawBuffers
                | _ ->
                    let! s = compilerState
                    yield Instruction.DepthMask 1
                    yield Instruction.StencilMask 0xFFFFFFFF
                    yield Instruction.DrawBuffers s.info.drawBufferCount s.info.drawBuffers

            yield
                textures |> Mod.map (fun textures ->
                    textures |> RefSet.toList |> List.collect (fun i ->
                        [
                            Instructions.setActiveTexture i
                            Instruction.BindSampler i 0
                            Instruction.BindTexture (int OpenGl.Enums.TextureTarget.Texture2D) 0
                        ]
                    )
                )

            yield
                ubos |> Mod.map (fun ubos ->
                    ubos |> RefSet.toList |> List.map (fun i ->
                        Instruction.BindBufferBase (int OpenGl.Enums.BufferTarget.UniformBuffer) i 0
                    )
                )

            yield Instruction.BindVertexArray 0
            yield Instruction.BindProgram 0
            yield Instruction.BindBuffer (int OpenTK.Graphics.OpenGL4.BufferTarget.DrawIndirectBuffer) 0

            
        }    

    let private toCode (s : CompilerState) =
        let myStats = ref FrameStatistics.Zero
        let stats = s.info.stats
//        let calls =
//            s.instructions |> List.map (fun i ->
//                match i.IsConstant with
//                    | true -> 
//                        let i = i.GetValue()
//                        let cnt = List.length i
//                        let dStats = { FrameStatistics.Zero with InstructionCount = float cnt; ActiveInstructionCount = float cnt }
//                        stats := !stats + dStats
//                        myStats := !myStats + dStats
//
//                        Mod.constant i
//
//                    | false -> 
//                        let mutable oldCount = 0
//                        i |> Mod.map (fun i -> 
//                            let newCount = List.length i
//                            let dCount = newCount - oldCount
//                            oldCount <- newCount
//                            let dStats = { FrameStatistics.Zero with InstructionCount = float dCount; ActiveInstructionCount = float dCount }
//
//                            stats := !stats + dStats
//                            myStats := !myStats + dStats
//                            i
//                        )
//            )
        let calls = s.instructions


        { new IAdaptiveCode<Instruction> with
            member x.Content = calls
            member x.Dispose() =    
                transact (fun () ->
                    for d in s.disposeActions do d()
                )

                for o in s.instructions do
                    for i in o.Inputs do
                        i.RemoveOutput o

                stats := !stats - !myStats
                myStats := FrameStatistics.Zero

        }


    /// <summary>
    /// compileDelta compiles all instructions needed to render [rj] 
    /// assuming [prev] was rendered immediately before.
    /// This function is the core-ingredient making our rendering-system
    /// fast as hell \o/.
    /// </summary>

    /// <summary>
    /// compileFull compiles all instructions needed to render [rj] 
    /// making no assumpltions about the previous GL state.
    /// </summary>




    let run (info : CompilerInfo) (c : Compiled<unit>) =
        let (s,()) =
            c.runCompile {
                info = info
                instructions = []
                disposeActions = []
            }

        s |> toCode


