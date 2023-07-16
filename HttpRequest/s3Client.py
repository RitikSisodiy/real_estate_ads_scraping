import aioboto3
import dotenv, os
from requests_html import AsyncHTMLSession
from os.path import basename
import io, asyncio, time, json
import uuid
from botocore.exceptions import ClientError

dotenv.load_dotenv()
from urllib.parse import urlsplit

cpath = os.path.dirname(__file__) or "."


class S3:
    def __init__(self, bucketname, folderpath="") -> None:
        """
        Initialize S3 object.

        Args:
            bucketname (str): Name of the S3 bucket.
            folderpath (str, optional): Folder path within the bucket. Defaults to "".
        """
        self.path = (
            folderpath and (folderpath if folderpath[-1] == "/" else folderpath + "/")
        ) or folderpath
        self.region = os.getenv("REGION_NAME")
        self.aiosession = aioboto3.Session()
        self.bucket = bucketname
        self.session = AsyncHTMLSession()
        self.filetype = json.load(open(f"{cpath}/filetype.json"))

    def getBaseName(self, url):
        # Get the base name from a URL.
        url = url.split("?")[0]
        return basename(urlsplit(url)[2])

    def isurl(self, url):
        # Check if a string is a URL.
        return url[:4].lower() == "http"

    def upload(self, path=""):
        # Upload a file to S3.
        if self.isurl(path):
            return self.uploadUrl(path)

    async def unique_file_name(self, file_name, file_extension):
        """
        Generate a unique file name for S3.

        Args:
            file_name (str): The original file name.
            file_extension (str): The file extension.

        Returns:
            str: The unique file name.
        """
        counter = 1
        while True:
            unique_file_path = f"{self.path}{file_name}_{counter}.{file_extension}"
            try:
                await self.s3.head_object(Bucket=self.bucket, Key=unique_file_path)
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return unique_file_path
                else:
                    raise
            counter += 1

    async def uploadUrl(self, s3, url, uploadPath):
        """
        Upload a file from a URL to S3.

        Args:
            s3: The S3 client object.
            url (str): The URL of the file to upload.
            uploadPath (str): The path to upload the file to.

        Returns:
            str: The uploaded file URL.
        """
        try:
            # Multipart upload
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
            }
            r = await self.session.get(url, headers=headers)
            # return if any error response
            if r.status_code is not 200:
                return url
            if r.headers.get("Content-Disposition"):
                # If the response has Content-Disposition, we take file name from it
                filename = r.headers["Content-Disposition"].split("filename=")[1]
                if filename[0] == '"' or filename[0] == "'":
                    filename = filename[1:-1]
            elif r.url and str(r.url) != url:
                # if we were redirected, the real file name we take from the final URL
                filename = self.getBaseName(str(r.url))
            else:
                filename = self.getBaseName(url)
            fileext = filename.rsplit(".", 1)
            randomfilename = str(uuid.uuid4())
            if len(fileext) < 2 or len(fileext[1]) > 5:
                extention = self.filetype.get(
                    (
                        r.headers.get("Content-Type")
                        and (r.headers.get("Content-Type").lower())
                        or ""
                    ),
                    "",
                ).replace(".", "")
            else:
                filename, extention = fileext
            filename = ".".join([randomfilename, extention])
            if uploadPath:
                filename = uploadPath + "/" + filename
            await s3.upload_fileobj(
                io.BytesIO(r.content),
                self.bucket,
                filename,
                ExtraArgs={"ACL": "public-read"},
            )
            return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{filename}"
        except:
            return url

    async def bulkUrlUpload(self, urls, uploadPath=""):
        """
        Upload multiple files from URLs to S3.

        Args:
            urls (list): List of URLs of the files to upload.
            uploadPath (str): The path to upload the files to.

        Returns:
            list: List of uploaded file URLs.
        """
        urlTask = []
        async with self.aiosession.client("s3", region_name=self.region) as s3:
            for url in urls:
                urlTask.append(
                    asyncio.ensure_future(self.uploadUrl(s3, url, uploadPath))
                )
            res = await asyncio.gather(*urlTask)
        return [url for url in res if url is not None]

    async def close(self):
        await self.session.close()


async def main():
    # Create S3 object with bucket name and folder path
    ob = S3("adimages-upload-scrapping-new", "test")
    imgs = [
        "http://thbr.figarocms.net/external/uRUmDsiK-59ZaktS5--lJp4YSb0=/0x1600/filters:fill(white):quality(80):strip_icc()/https%3A%2F%2Fpasserelle.static.iadfrance.com%2Fphotos%2Frealestate%2F2023-01%2Fproduct-1278136-1.jpg%3Fbridge%3Dexplorimmo%26ts%3D202302020002"
    ]
    start_time = time.time()
    # Upload images to S3
    urls = await ob.bulkUrlUpload(imgs, uploadPath="leboncoin")
    await ob.close()
    diff = time.time() - start_time
    print(urls)
    print(
        f"{diff} seconds for {len(imgs)} files {(diff/len(imgs))} seconds for each files"
    )


if __name__ == "__main__":
    asyncio.run(main())
