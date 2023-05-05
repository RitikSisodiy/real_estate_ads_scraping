from io import BytesIO
from PIL import Image
import imagehash
import asyncio
import aiohttp

class ImageHash:
    def __init__(self):
        # Initialize aiohttp client session
        self.session = aiohttp.ClientSession()
        # Set retry limit for failed requests
        self.retry_limit = 3
        
    async def fetch(self, url, retry=0):
        # Print URL being fetched for debugging
        print(url)
        try:
            # Send GET request using aiohttp client session with a timeout of 10 seconds
            async with self.session.get(url=url, timeout=10) as r:
                # Raise exception for any errors in response
                r.raise_for_status()
                # Read the response content as bytes
                content = await r.read()
        except :
            # Retry request if it failed and retry limit hasn't been reached
            if retry >= self.retry_limit:
                return None
            retry += 1
            return await self.fetch(url, retry)
        # Return the response content as bytes
        return content
        
    async def getHash(self, url):
        # Fetch the content of the image at the URL
        content = await self.fetch(url)
        # If fetching the content was successful
        if content:
            try:
                # Open the image using PIL Image module
                img = Image.open(BytesIO(content))
                # Calculate the hash of the image using imagehash module and return as string
                return str(imagehash.average_hash(img))
            except (OSError, ValueError):
                # If there was an error in opening the image or calculating its hash, do nothing
                pass
    
    async def getHashByUrl(self, doc, imagelipath="images_url", destpath="imagehash", binary=False):
        # Create a list of tasks to fetch and calculate hash of all images
        if doc.get(imagelipath):
            if doc.get(imagelipath):
                tasks = [asyncio.ensure_future(self.getHash(url)) for url in doc[imagelipath]]
            else:return doc
            # Gather all the results from the tasks and filter out None values
            doc[destpath] = [hash_value for hash_value in await asyncio.gather(*tasks) if hash_value is not None]
            # Return the document with the image hashes added to it
        return doc
