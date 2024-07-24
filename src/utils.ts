import { Readable } from "stream";

/**
 * @param chunkSize The size of each chunk in bytes. Defaults to 64KB.
 */
export function blobToReadable(blob: Blob, chunkSize = 1024 * 64) {
    let offset = 0;

    return new Readable({
        async read() {
            if (offset >= blob.size) {
                this.push(null); // Signal the end of the stream
                return;
            }

            const end = Math.min(offset + chunkSize, blob.size);
            const chunk = blob.slice(offset, end);
            const arrayBuffer = await chunk.arrayBuffer();
            this.push(Buffer.from(arrayBuffer));

            offset = end;
        },
    });
}

export async function streamToString(stream: Readable): Promise<string> {
    return await new Promise((resolve, reject) => {
        const chunks: Uint8Array[] = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    });
}

export function xserializeMetadata(metadata: Record<string, any>): Record<string, string> {
    if (!metadata || typeof metadata !== "object") return {};
    return Object.entries(metadata || {}).reduce((acc, [key, value]) => {
        if (value === undefined) return acc;
        acc[key] = value.toString();
        return acc;
    }, {} as Record<string, string>);
}
