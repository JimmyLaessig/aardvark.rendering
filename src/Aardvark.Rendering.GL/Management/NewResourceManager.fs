﻿namespace Aardvark.Rendering.GL

#nowarn "9"
#nowarn "51"

open System
open System.Threading
open System.Collections.Concurrent
open Microsoft.FSharp.NativeInterop
open Aardvark.Base
open Aardvark.Base.Incremental
open Aardvark.Base.Rendering
open Aardvark.Rendering.GL

type RefCountedResource<'h>(create : unit -> 'h, delete : 'h -> unit) =
    let mutable refCount = 0
    let mutable handle = Unchecked.defaultof<'h>

    member x.Acquire() = 
        if Interlocked.Increment &refCount = 1 then
            handle <- create()

    member x.Release() = 
        if Interlocked.Decrement &refCount = 0 then
            delete handle
            handle <- Unchecked.defaultof<_>

    member x.Handle =
        if refCount <= 0 then 
            failwith "[RefCountedResource] ref count zero"

        handle

// TODO:
// 1) Buffer/Texture sharing
// 2) NullBuffers
// 3) NullTextures
type BufferSharing(ctx : Context) =
    let cache = ConcurrentDictionary<IBuffer, RefCountedResource<Buffer>>()

    let get (b : IBuffer) =
        cache.GetOrAdd(b, fun v -> 
            RefCountedResource<_>(
                (fun () -> ctx.CreateBuffer b),
                (ctx.Delete)
            )
        )

    member x.Create(data : IBuffer) : RefCountedResource<Buffer> =
        let shared = get data
        shared.Acquire()
        shared

    member x.Update(b : RefCountedResource<Buffer>, data : IBuffer) : RefCountedResource<Buffer> =
        let newShared = get data
        if newShared = b then
            b
        else
            newShared.Acquire()
            b.Release()
            newShared

    member x.Delete(b : RefCountedResource<Buffer>) =
        b.Release()


type UniformBufferView =
    class
        val mutable public Buffer : IMod<IBuffer>
        val mutable public Offset : nativeint
        val mutable public Size : nativeint

        new(b,o,s) = { Buffer = b; Offset = o; Size = s }

    end

type UniformBufferManager(ctx : Context, size : int, fields : list<ActiveUniform>) =
  
    static let singleStats = { FrameStatistics.Zero with ResourceUpdateCount = 1.0; ResourceUpdateCounts = Map.ofList [ResourceKind.UniformBufferView, 1.0] }
    
    let alignedSize = (size + 255) &&& ~~~255

    let buffer = new MappedBuffer(ctx)
    let manager = MemoryManager.createNop()

    let viewCache = ResourceCache<UniformBufferView>()
    let rw = new ReaderWriterLockSlim()

    member x.CreateUniformBuffer(scope : Ag.Scope, u : IUniformProvider, additional : SymbolDict<obj>) : IResource<UniformBufferView> =
        let values =
            fields |> List.map (fun f ->
                let sem = Symbol.Create f.semantic
                match u.TryGetUniform(scope, sem) with
                    | Some v -> sem, v
                    | None -> 
                        match additional.TryGetValue sem with
                            | (true, (:? IMod as m)) -> sem, m
                            | _ -> failwithf "[GL] could not get uniform: %A" f
            )

        let key = values |> List.map (fun (_,v) -> v :> obj)

        viewCache.GetOrCreate(
            key,
            fun () ->
                let values = values |> List.map (fun (s,v) -> s, v :> IAdaptiveObject) |> Map.ofList
                let uniformFields = fields |> List.map (fun a -> a.UniformField)
                let writers = UnmanagedUniformWriters.writers true uniformFields values
     
                let mutable block = Unchecked.defaultof<_>
                { new Resource<UniformBufferView>() with
                    member x.Create old =
                        let handle = 
                            match old with
                                | Some old -> old
                                | None ->
                                    block <- manager.Alloc alignedSize
                                    ReaderWriterLock.write rw (fun () ->
                                        if buffer.Capacity <> manager.Capacity then buffer.Resize(manager.Capacity)
                                    )
                                    UniformBufferView(buffer, block.Offset, nativeint block.Size)

                        ReaderWriterLock.read rw (fun () ->
                            buffer.Use(handle.Offset, handle.Size, fun ptr ->
                                for (_,w) in writers do w.Write(x, ptr)
                            )
                        )
                        handle, singleStats

                    member x.Destroy h =
                        manager.Free block
                        if manager.AllocatedBytes = 0 then
                            buffer.Resize 0

                }
        )

    member x.Dispose() =
        buffer.Dispose()
        manager.Dispose()


type ResourceManagerNew(ctx : Context) =
    static let updateStats (kind : ResourceKind) =
        { FrameStatistics.Zero with ResourceUpdateCount = 1.0; ResourceUpdateCounts = Map.ofList [kind, 1.0] }

    static let bufferUpdateStats = updateStats ResourceKind.Buffer
    static let textureUpdateStats = updateStats ResourceKind.Texture
    static let programUpdateStats = updateStats ResourceKind.ShaderProgram
    static let samplerUpdateStats = updateStats ResourceKind.SamplerState
    static let vaoUpdateStats = updateStats ResourceKind.VertexArrayObject
    static let uniformLocationUpdateStats = updateStats ResourceKind.UniformLocation


    let bufferCache = ResourceCache<Buffer>()
    let textureCache = ResourceCache<Texture>()
    let indirectBufferCache = ResourceCache<IndirectBuffer>()
    let programCache = ResourceCache<Program>()
    let samplerCache = ResourceCache<Sampler>()
    let vaoCache = ResourceCache<VertexArrayObject>()
    let uniformLocationCache = ResourceCache<UniformLocation>()

    let uniformBufferManagers = ConcurrentDictionary<int * list<ActiveUniform>, UniformBufferManager>()



    member x.CreateBuffer(data : IMod<IBuffer>) =
        match data with
            | :? IAdaptiveBuffer as data ->
                bufferCache.GetOrCreate(
                    [data :> obj],
                    fun () ->
                        let mutable r = Unchecked.defaultof<_>
                        { new Resource<Buffer>() with
                            member x.Create (old : Option<Buffer>) =
                                match old with
                                    | None ->
                                        r <- data.GetReader()
                                        let (nb, _) = r.GetDirtyRanges(x)
                                        ctx.CreateBuffer(nb), bufferUpdateStats
                                    | Some old ->
                                        let (nb, ranges) = r.GetDirtyRanges(x)
                                        nb.Use (fun ptr ->
                                            ctx.UploadRanges(old, ptr, ranges)
                                        )
                                        old, bufferUpdateStats

                            member x.Destroy(b : Buffer) =
                                ctx.Delete b
                                r.Dispose()
                        }
                )

            | _ ->
                bufferCache.GetOrCreate<IBuffer>(data, {
                    create = fun b      -> ctx.CreateBuffer b
                    update = fun h b    -> ctx.Upload(h, b); h
                    delete = fun h      -> ctx.Delete h
                    stats  = bufferUpdateStats
                })

    member x.CreateTexture(data : IMod<ITexture>) =
        textureCache.GetOrCreate<ITexture>(data, {
            create = fun b      -> ctx.CreateTexture b
            update = fun h b    -> ctx.Upload(h, b); h
            delete = fun h      -> ctx.Delete h
            stats  = textureUpdateStats
        })

    member x.CreateIndirectBuffer(indexed : bool, data : IMod<IBuffer>) =
        indirectBufferCache.GetOrCreate<IBuffer>(data, {
            create = fun b      -> ctx.CreateIndirect(indexed, b)
            update = fun h b    -> ctx.UploadIndirect(h, indexed, b); h
            delete = fun h      -> ctx.Delete h
            stats  = bufferUpdateStats
        })

    member x.CreateSurface(signature : IFramebufferSignature, surface : IMod<ISurface>) =
        let create (s : ISurface) =
            match SurfaceCompilers.compile ctx signature s with
                | Success program -> program
                | Error e -> failwithf "[GL] surface compilation failed: %s" e

        programCache.GetOrCreate<ISurface>(surface, {
            create = fun b      -> create b
            update = fun h b    -> ctx.Delete(h); create b
            delete = fun h      -> ctx.Delete h
            stats  = programUpdateStats
        })

    member x.CreateSampler (sam : IMod<SamplerStateDescription>) =
        samplerCache.GetOrCreate<SamplerStateDescription>(sam, {
            create = fun b      -> ctx.CreateSampler b
            update = fun h b    -> ctx.Update(h,b); h
            delete = fun h      -> ctx.Delete h
            stats  = samplerUpdateStats
        })

    member x.CreateVertexArrayObject( bindings : list<int * BufferView * AttributeFrequency * IResource<Buffer>>, index : Option<IResource<Buffer>>) =
        let createView (self : IAdaptiveObject) (index : int, view : BufferView, frequency : AttributeFrequency, buffer : IResource<Buffer>) =
            index, { 
                Type = view.ElementType
                Frequency = frequency
                Normalized = false; 
                Stride = view.Stride
                Offset = view.Offset
                Buffer = buffer.Handle.GetValue self
            }

        vaoCache.GetOrCreate(
            [ bindings :> obj; index :> obj ],
            fun () ->
                { new Resource<VertexArrayObject>() with
                    member x.Create (old : Option<VertexArrayObject>) =
                        let attributes = bindings |> List.map (createView x)
                        let index = match index with | Some i -> i.Handle.GetValue x |> Some | _ -> None
                        
                        match old with
                            | Some old ->
                                match index with
                                    | Some i -> ctx.Update(old, i, attributes)
                                    | None -> ctx.Update(old, attributes)
                                old, vaoUpdateStats

                            | None ->
                                let handle = 
                                    match index with
                                        | Some i -> ctx.CreateVertexArrayObject(i, attributes)
                                        | None -> ctx.CreateVertexArrayObject(attributes)

                                handle, vaoUpdateStats
                        
                    member x.Destroy vao =
                        ctx.Delete vao
                }
        )

    member x.CreateUniformLocation(scope : Ag.Scope, u : IUniformProvider, uniform : ActiveUniform) =
        match u.TryGetUniform (scope, Sym.ofString uniform.semantic) with
            | Some v ->
                uniformLocationCache.GetOrCreate(
                    [v :> obj],
                    fun () ->
                        let inputs = Map.ofList [Symbol.Create uniform.semantic, v :> IAdaptiveObject]
                        let _,writer = UnmanagedUniformWriters.writers false [uniform.UniformField] inputs |> List.head
     
                        { new Resource<UniformLocation>() with
                            member x.Create old =
                                let handle =
                                    match old with 
                                        | Some o -> o
                                        | None -> ctx.CreateUniformLocation(uniform.uniformType.SizeInBytes, uniform.uniformType)
                                
                                writer.Write(x, handle.Data)
                                handle, uniformLocationUpdateStats

                            member x.Destroy h =
                                ctx.Delete h
                        }        
                )
                

            | None ->
                failwithf "[GL] could not get uniform: %A" uniform
     
    member x.CreateUniformBuffer(scope : Ag.Scope, layout : UniformBlock, program : Program, u : IUniformProvider) =
        let manager = 
            uniformBufferManagers.GetOrAdd((layout.size, layout.fields), fun (s,f) -> new UniformBufferManager(ctx, s, f))

        manager.CreateUniformBuffer(scope, u, program.UniformGetters)
      