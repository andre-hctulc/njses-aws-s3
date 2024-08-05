import {
    _Object,
    CopyObjectCommand,
    CopyObjectCommandInput,
    Delete,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    DeleteObjectsCommandInput,
    GetObjectCommand,
    GetObjectCommandInput,
    GetObjectCommandOutput,
    HeadObjectCommand,
    HeadObjectCommandInput,
    HeadObjectCommandOutput,
    ListObjectsCommand,
    ListObjectsCommandInput,
    ListObjectsCommandOutput,
    PutObjectCommand,
    PutObjectCommandInput,
    S3Client,
    S3ClientConfig,
} from "@aws-sdk/client-s3";
import type { Readable } from "stream";
import { Service } from "../../njses/src/decorators";
import { blobToReadable, streamToString } from "./utils";

interface FetchHeadsOptions {
    marker?: string;
    prefix?: string;
    limit?: number;
}

type GetObjectCommandOutputBody = Exclude<GetObjectCommandOutput["Body"], undefined> | null;

export type Metadata = Record<string, string>;
export type ObjectData = string | Buffer | Uint8Array | Readable | ArrayBuffer | Blob;

export type AWSS3BucketConfig<M extends Metadata = Metadata> = {
    client: S3ClientConfig | S3Client;
    /** @default { ...metadata1, ...metadata2 } */
    mergeMetadata?: (metadata1: M, metadata2: Partial<M>) => M;
};

@Service({ name: "$$aws_s3_bucket" })
export class AWSS3Bucket<M extends Metadata = Metadata> {
    readonly name: string;
    readonly client: S3Client;
    private _config: AWSS3BucketConfig<M>;

    constructor(bucketName: string, config: AWSS3BucketConfig<M>) {
        this.name = bucketName;
        this._config = config;
        this.client = config.client instanceof S3Client ? config.client : new S3Client(config.client);
    }

    // -- Objects

    async getRaw(key: string, input?: Partial<GetObjectCommandInput>): Promise<GetObjectCommandOutput> {
        const command = new GetObjectCommand({
            Bucket: this.name,
            Key: key,
            ...input,
        });
        const s3response = await this.client.send(command);
        return s3response;
    }

    async get(key: string): Promise<GetObjectCommandOutputBody> {
        const s3response = await this.getRaw(key);
        return s3response.Body || null;
    }

    async getText(key: string) {
        const s3response = await this.getRaw(key);
        if (!s3response.Body) return "";
        return await streamToString(s3response.Body as Readable);
    }

    async putRaw(key: string, input?: Partial<PutObjectCommandInput>) {
        const command = new PutObjectCommand({
            Bucket: this.name,
            Key: key,
            ...input,
        });
        return this.client.send(command);
    }

    async put(key: string, data: ObjectData) {
        if (data instanceof ArrayBuffer) data = Buffer.from(data);
        else if (data instanceof Blob) data = await blobToReadable(data);
        await this.putRaw(key, { Body: data as any });
    }

    async deleteRaw(key: string) {
        const command = new DeleteObjectCommand({
            Bucket: this.name,
            Key: key,
        });
        return this.client.send(command);
    }

    delete(key: string) {
        return this.deleteRaw(key);
    }

    async deleteManyRaw(del: Delete, input: Partial<DeleteObjectsCommandInput> = {}) {
        const command = new DeleteObjectsCommand({ Bucket: this.name, Delete: del, ...input });
        return this.client.send(command);
    }

    deleteMany(keys: string[]) {
        return this.deleteManyRaw({ Objects: keys.map((k) => ({ Key: k })) });
    }

    async copyRaw(oldKey: string, newKey: string, params?: Partial<CopyObjectCommandInput>) {
        const cmd = new CopyObjectCommand({
            Bucket: this.name,
            CopySource: `${this.name}/${oldKey}`,
            // rename
            Key: newKey,
            ...params,
        });
        return this.client.send(cmd);
    }

    async rename(oldKey: string, newKey: string) {
        if (oldKey === newKey) return;
        await this.copyRaw(oldKey, newKey);
        await this.deleteRaw(oldKey);
    }

    // -- Head

    async getHeadRaw(key: string, input?: Partial<HeadObjectCommandInput>): Promise<HeadObjectCommandOutput> {
        const command = new HeadObjectCommand({
            Bucket: this.name,
            Key: key,
            ...input,
        });
        return this.client.send(command);
    }

    async getHead(key: string): Promise<M> {
        const s3response = await this.getHeadRaw(key);
        return (s3response.Metadata as any) || {};
    }

    async getHeadsRaw(input?: Partial<ListObjectsCommandInput>): Promise<ListObjectsCommandOutput> {
        // max. MaxKeys is 1000
        const command = new ListObjectsCommand({
            Bucket: this.name,
            ...input,
        });
        return await this.client.send(command);
    }

    async getHeads(options: FetchHeadsOptions = {}) {
        const s3response = await this.getHeadsRaw({
            Prefix: options.prefix,
            Marker: options.marker,
            MaxKeys: options.limit,
        });
        return s3response.Contents || [];
    }

    async putHead(key: string, metadata: Partial<M>) {
        const currentMetadeata = await this.getHead(key);
        const newMetadata = this._config.mergeMetadata
            ? this._config.mergeMetadata(currentMetadeata, metadata)
            : { ...currentMetadeata, ...metadata };
        await this.copyRaw(key, key, {
            MetadataDirective: "REPLACE",
            Metadata: newMetadata,
        });
    }
}
