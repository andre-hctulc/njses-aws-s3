import {
    CreateBucketCommand,
    CreateBucketCommandInput,
    DeleteBucketCommand,
    ListBucketsCommand,
    S3Client,
    S3ClientConfig,
} from "@aws-sdk/client-s3";
import { App, Service } from "../../njses";
import { AWSS3Bucket, AWSS3BucketConfig, Metadata } from "./bucket";

@Service({ name: "$$aws_s3" })
export class AWSS3Client {
    readonly raw: S3Client;

    constructor(config: S3ClientConfig) {
        this.raw = new S3Client(config);
    }

    close() {
        return this.raw.destroy();
    }

    async deleteBucket(bucketName: string): Promise<void> {
        const command = new DeleteBucketCommand({ Bucket: bucketName });
        await this.raw.send(command);
    }

    async listBuckets() {
        const command = new ListBucketsCommand({});
        const response = await this.raw.send(command);
        return response;
    }

    async createBucket<M extends Metadata>(
        bucketName: string,
        params: Omit<CreateBucketCommandInput, "Bucket">,
        config?: Omit<AWSS3BucketConfig<M>, "client">
    ): Promise<AWSS3Bucket<M>> {
        const createCommand = new CreateBucketCommand({ ...params, Bucket: bucketName });
        await this.raw.send(createCommand);
        return this.getBucket(bucketName, config);
    }

    getBucket<M extends Metadata>(
        bucketName: string,
        config?: Omit<AWSS3BucketConfig<M>, "client">
    ): Promise<AWSS3Bucket<M>> {
        return App.injectX([AWSS3Bucket<M>, bucketName, { ...config, client: this.raw }]);
    }
}
