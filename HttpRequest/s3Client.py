import aioboto3
import dotenv,os
from requests_html import AsyncHTMLSession
from os.path import basename
import io ,asyncio,time,json
import uuid
from botocore.exceptions import ClientError
dotenv.load_dotenv()
from urllib.parse import urlsplit
cpath =os.path.dirname(__file__) or "."
class S3:
    # aiosession = aioboto3.Session()
    # session = aiohttp.ClientSession()
    def __init__(self,bucketname,folderpath="") -> None:
        self.path = (folderpath and (folderpath if folderpath[-1]=="/"  else folderpath+"/")) or folderpath
        self.region = os.getenv("REGION_NAME")
        # self.s3 = session.client('s3',region_name=self.region)
        # self.aiosession = aioboto3.Session()
        self.aiosession = aioboto3.Session()
        self.bucket = bucketname
        # self.session = aiohttp.ClientSession()
        self.session = AsyncHTMLSession()
        self.filetype = json.load(open(f"{cpath}/filetype.json"))
        
    def getBaseName(self,url):
        url = url.split("?")[0]
        return basename(urlsplit(url)[2])
    def isurl(self,url):
        return url[:4].lower()=="http"
    def upload(self,path=""):
        if self.isurl(path):
            return self.uploadUrl(path)
    async def unique_file_name(self,file_name, file_extension):
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
            counter+=1
    async def uploadUrl(self,s3,url,uploadPath):
        try:
            # Multipart upload
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
            }
            r = await self.session.get(url,headers=headers)
            # return if any error response
            if r.status_code is not 200:
                return url
            if r.headers.get('Content-Disposition'):
                # If the response has Content-Disposition, we take file name from it
                filename = r.headers['Content-Disposition'].split('filename=')[1]
                if filename[0] == '"' or filename[0] == "'":
                    filename = filename[1:-1]
            elif r.url and str(r.url) != url: 
                # if we were redirected, the real file name we take from the final URL
                filename = self.getBaseName(str(r.url))
            else:
                filename = self.getBaseName(url)
            fileext = filename.rsplit(".",1)
            randomfilename = str(uuid.uuid4())
            if len(fileext)<2 or len(fileext[1])>5:
                extention =   self.filetype.get((r.headers.get("Content-Type") and (r.headers.get("Content-Type").lower() ) or ""),"").replace(".","")
            else:
                filename, extention = fileext
            filename =".".join([randomfilename,extention])
            if uploadPath:filename = uploadPath+"/"+filename
            # print(f"Uploading {filename} to s3")
            await s3.upload_fileobj(io.BytesIO(r.content), self.bucket, filename,ExtraArgs={'ACL': 'public-read'})
            # print(f"Finished Uploading {filename} to s3")
            return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{filename}"
        except:return url
    async def bulkUrlUpload(self,urls,uploadPath=""):
        urlTask =[]
        async with self.aiosession.client("s3",region_name=self.region) as s3:
            for url in urls:
                urlTask.append(asyncio.ensure_future(self.uploadUrl(s3,url,uploadPath)))
            res =  await asyncio.gather(*urlTask)
        return [url for url in res if url is not None]
    async def close(self):
        await self.session.close()

#for testing the class
async def main():
    ob =  S3("adimages-upload-scrapping-new","test")
    imgs =  [
			"http://thbr.figarocms.net/external/uRUmDsiK-59ZaktS5--lJp4YSb0=/0x1600/filters:fill(white):quality(80):strip_icc()/https%3A%2F%2Fpasserelle.static.iadfrance.com%2Fphotos%2Frealestate%2F2023-01%2Fproduct-1278136-1.jpg%3Fbridge%3Dexplorimmo%26ts%3D202302020002"
	]
    start_time = time.time()
    urls = await  ob.bulkUrlUpload(imgs,uploadPath="leboncoin")
    ## close all connection
    await ob.close()
    diff = (time.time() - start_time)
    print(urls)
    print(f"{diff} seconds for {len(imgs)} files {(diff/len(imgs))} seconds for each files")
if __name__=="__main__":
    asyncio.run(main())
