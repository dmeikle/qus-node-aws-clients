/*
 * MIT License
 * 
 * Copyright (c) 2024 Quantum Unit Solutions
 * Author: David Meikle
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
import {
    S3Client,
    GetObjectCommand,
    GetObjectCommandOutput,
    ListObjectsV2Command,
    PutObjectCommand,
    ListObjectsV2CommandOutput
} from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import {Injectable} from "@nestjs/common";

@Injectable()
export class S3Service {



    constructor(
        protected readonly s3Client: S3Client,
        protected region: string) {
    }

    /**
     * Uploads a file to S3.
     *
     * @param bucketName
     * @param filePath
     * @param data
     * @param mimeType
     */
    async uploadToS3(bucketName: string, filePath: string, data: any, mimeType: string): Promise<any> {
        const input: any = {
            Bucket: bucketName,
            Key: filePath,
            Body: data,
            ContentType: mimeType,
        };
        const command: any = new PutObjectCommand(input);

        try {
            return await this.s3Client.send(command);
        } catch (error: any) {
            throw new Error(`Error uploading to S3: ${error.message}`);
        }
    }


    /**
     * Retrieves an item from S3.
     *
     * @param bucketName
     * @param filePath
     */
    async getFromS3(bucketName: string, filePath: string): Promise<string> {
        const input = {
            Bucket: bucketName,
            Key: filePath,
        };
        const command = new GetObjectCommand(input);

        try {
            const response: GetObjectCommandOutput = await this.s3Client.send(command);

            if (response.Body instanceof Readable) {
                const chunks: Uint8Array[] = [];
                for await (const chunk of response.Body) {
                    chunks.push(chunk);
                }
                // Concatenate all chunks into a single Buffer
                return Buffer.concat(chunks).toString('utf-8');
            } else {
                throw new Error('Unexpected response body type');
            }
        } catch (error: any) {
            throw new Error(`Error retrieving from S3: ${error.message}`);
        }
    }

    async getFromS3AsBinary(bucketName: string, filePath: string): Promise<Buffer> {
        const input = {
            Bucket: bucketName,
            Key: filePath,
        };
        const command = new GetObjectCommand(input);

        try {
            const response: GetObjectCommandOutput = await this.s3Client.send(command);

            if (response.Body instanceof Readable) {
                const chunks: Uint8Array[] = [];
                for await (const chunk of response.Body) {
                    chunks.push(chunk);
                }
                return Buffer.concat(chunks);
            } else {
                throw new Error('Unexpected response body type');
            }
        } catch (error: any) {
            throw new Error(`Error retrieving from S3: ${error.message}`);
        }
    }


    /**
     * Retrieves the most recent file from S3 based on a prefix.
     *
     * @param bucketName
     * @param prefix
     */
    async getMostRecentFile(bucketName: string, prefix: string): Promise<string | undefined> {
        const command = new ListObjectsV2Command({ Bucket: bucketName, Prefix: prefix });
        const response: ListObjectsV2CommandOutput = await this.s3Client.send(command);

        if (!response.Contents || response.Contents.length === 0) {
            console.log('No files found.');
            return undefined;
        }

        // Extract filenames and sort by date
        const files: string[] = response.Contents.map((item) => item.Key!).filter((key) => key.includes('_'));
        const sortedFiles: string[] = files.sort((a, b) => {
            const dateA = new Date(a.slice(-14, -4)); // Extract last 10 characters before '.pdf'
            const dateB = new Date(b.slice(-14, -4));
            return dateB.getTime() - dateA.getTime();
        });

        return sortedFiles[0]; // Most recent file
    }
}