from .scrapProxy import ProxyScraper
import time
import os 
cpath =os.path.dirname(__file__) or "."
def updateProxyList(interval=300):
    SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security/register"
    headers = {
                'User-Agent': 'okhttp/4.6.0',
            }
    prox = ProxyScraper(SELOGER_SECURITY_URL,headers)
    # while True:
    prox.FetchNGetProxy()
    prox.save(cpath)
    time.sleep(interval)