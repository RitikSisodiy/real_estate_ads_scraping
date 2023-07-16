import time
import traceback
import os

cpath = os.path.dirname(__file__) or "."


def fetch(url, session, Json=False, file=False, retry=0, **kwargs):
    """
    Fetches data from a given URL using the provided session object and additional options.

    Args:
        url (str): The URL to fetch the data from.
        session: The session object used for making the HTTP request.
        Json (bool): Flag indicating whether the response should be parsed as JSON. Default is False.
        file (bool): Flag indicating whether the response should be treated as a file. Default is False.
        retry (int): The number of times to retry the request in case of failure. Default is 0.
        **kwargs: Additional keyword arguments to pass to the fetch function.

    Returns:
        dict or str or None: The fetched data, parsed as JSON if Json=True, or raw HTML if file=False and Json=False.
                             Returns an empty dictionary if the response status code is 404.
                             Returns None if the response status code is 500.
    """
    print(url)
    if not kwargs.get("headers"):
        kwargs["headers"] = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        }
    if retry >= 3:
        return
    retry += 1
    try:
        try:
            response = session.fetch(url, **kwargs)
        except:
            traceback.print_exc()
            time.sleep(1)
            return fetch(url, session, Json, file, retry=retry, **kwargs)
        try:
            if response.status_code == 404:
                return {}
            if response.status_code == 500:
                return None
            if response.status_code == 200:
                if Json:
                    return response.json()
                html = response.html
                return html
            else:
                time.sleep(1)
                return fetch(url, session, Json, file, retry=retry, **kwargs)
        except Exception as e:
            print(e, "got some isssue1")
    except Exception as e:
        pass
