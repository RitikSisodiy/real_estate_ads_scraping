from io import BytesIO
from PIL import Image
import imagehash
import asyncio
import aiohttp


class ImageHash:
    def __init__(self):
        # Create a session for making asynchronous HTTP requests
        self.session = aiohttp.ClientSession()
        self.retry_limit = 3

    async def fetch(self, url, retry=0):
        """
        Fetch the content of an image from the specified URL.

        Args:
            url (str): URL of the image.
            retry (int): Number of retries (used for recursion).

        Returns:
            The content of the image as bytes, or None if the request fails.
        """
        print(url)
        try:
            async with self.session.get(url=url, timeout=10) as r:
                r.raise_for_status()
                content = await r.read()
        except:
            if retry >= self.retry_limit:
                return None
            retry += 1
            return await self.fetch(url, retry)
        return content

    async def getHash(self, url):
        """
        Calculate the image hash for the image at the specified URL.

        Args:
            url (str): URL of the image.

        Returns:
            The image hash as a string, or None if the image cannot be processed.
        """
        content = await self.fetch(url)
        if content:
            try:
                img = Image.open(BytesIO(content))
                return str(imagehash.average_hash(img))
            except (OSError, ValueError):
                pass

    async def getHashByUrl(
        self, doc, imagelipath="images_url", destpath="imagehash", binary=False
    ):
        """
        Calculate the image hashes for a list of image URLs in a document.

        Args:
            doc (dict): The document containing the image URLs.
            imagelipath (str): Path to the list of image URLs in the document.
            destpath (str): Path to store the image hashes in the document.
            binary (bool): Flag indicating whether to calculate binary hashes.

        Returns:
            The updated document with image hashes.
        """
        if doc.get(imagelipath):
            if doc.get(imagelipath):
                tasks = [
                    asyncio.ensure_future(self.getHash(url)) for url in doc[imagelipath]
                ]
            else:
                return doc
            doc[destpath] = [
                hash_value
                for hash_value in await asyncio.gather(*tasks)
                if hash_value is not None
            ]
        return doc
