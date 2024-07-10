import {
    _Object,
    CopyObjectCommand,
    CopyObjectCommandInput,
    DeleteObjectCommand,
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
import { Readable } from "stream";

async function streamToString(stream: Readable): Promise<string> {
    return await new Promise((resolve, reject) => {
        const chunks: Uint8Array[] = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    });
}

function serializeMetadata(metadata: Record<string, any>): Record<string, string> {
    if (!metadata || typeof metadata !== "object") return {};
    return Object.entries(metadata || {}).reduce((acc, [key, value]) => {
        if (value === undefined) return acc;
        acc[key] = value.toString();
        return acc;
    }, {} as Record<string, string>);
}

type ResponseMetadata<M extends object> = { [K in keyof M]?: string };

interface FetchHeadsOptions {
    marker?: string;
    prefix?: string;
    limit?: number;
}

type GetObjectCommandOutputBody = Exclude<GetObjectCommandOutput["Body"], undefined> | null;

type S3ConnectionConfig<M extends object = Record<string, string>> = {
    client: S3ClientConfig | S3Client;
    mergeMetadata?: (m1: ResponseMetadata<M>, m2: ResponseMetadata<M>) => M;
};

/**
 * @template M Metadata
 */
export class S3Connection<M extends object = Record<string, string>> {
    readonly bucketName: string;
    private _client: S3Client;
    private _config: S3ConnectionConfig<M>;

    constructor(bucketName: string, config: S3ConnectionConfig<M>) {
        this.bucketName = bucketName;
        this._config = config;
        this._client = config.client instanceof S3Client ? config.client : new S3Client(config.client);
    }

    // -- Objects

    async getRaw(key: string, input?: Partial<GetObjectCommandInput>): Promise<GetObjectCommandOutput> {
        const command = new GetObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
        const s3response = await this._client.send(command);
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
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
        return this._client.send(command);
    }

    async put(key: string, data: string | Buffer | Uint8Array | Readable) {
        await this.putRaw(key, { Body: data });
    }

    async delRaw(key: string) {
        const command = new DeleteObjectCommand({
            Bucket: this.bucketName,
            Key: key,
        });
        return this._client.send(command);
    }

    async del(key: string) {
        await this.delRaw(key);
    }

    async copyRaw(oldKey: string, newKey: string, params?: Partial<CopyObjectCommandInput>) {
        const cmd = new CopyObjectCommand({
            Bucket: this.bucketName,
            CopySource: `${this.bucketName}/${oldKey}`,
            // rename
            Key: newKey,
            ...params,
        });
        return this._client.send(cmd);
    }

    async rename(oldKey: string, newKey: string) {
        if (oldKey === newKey) return;
        await this.copyRaw(oldKey, newKey);
        await this.delRaw(oldKey);
    }

    // -- Head

    async getHeadRaw(key: string, input?: Partial<HeadObjectCommandInput>): Promise<HeadObjectCommandOutput> {
        const command = new HeadObjectCommand({
            Bucket: this.bucketName,
            Key: key,
            ...input,
        });
        return this._client.send(command);
    }

    async getHead(key: string): Promise<ResponseMetadata<M>> {
        const s3response = await this.getHeadRaw(key);
        return (s3response.Metadata as any) || {};
    }

    async getHeadsRaw(input?: Partial<ListObjectsCommandInput>): Promise<ListObjectsCommandOutput> {
        // max. MaxKeys is 1000
        const command = new ListObjectsCommand({
            Bucket: this.bucketName,
            ...input,
        });
        return await this._client.send(command);
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
            ? this._config.mergeMetadata(currentMetadeata, serializeMetadata(metadata))
            : metadata;
        await this.copyRaw(key, key, {
            MetadataDirective: "REPLACE",
            Metadata: serializeMetadata(newMetadata),
        });
    }
}
