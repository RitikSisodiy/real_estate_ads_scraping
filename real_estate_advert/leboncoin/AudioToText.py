import speech_recognition as sr
from pydub import AudioSegment
import os
import requests
def convTowav(filename):
    dst = "test.wav"
    sound = AudioSegment.from_mp3(filename)
    sound.export(dst, format="wav")
    return dst
def DownloadFile(url,filename): 
    r = requests.get(url, allow_redirects=True)
    try:
        open(filename, 'wb').write(r.content)
    except: pass
    return convTowav(filename)
    
def GetAudioToNumber(url):
    filename = url[url.rfind('/')+1:]
    dst = DownloadFile(url,filename)
    num = FileToText(dst,filename)
    return num
def GetNumFromText(text):
    num = ''
    for t in text:
        try:
            a = int(t)
            num += t
        except: pass
    print(num)
    for i in range(0,9):
        num = num.replace(f"{i}{i}",f"{i}")
    return num
def FileToText(dst,filename=None):
    if ".mp3" in dst:
        filename = dst
        dst = convTowav(dst)
    r = sr.Recognizer()
    with sr.AudioFile(dst) as source:
        # listen for the data (load audio to memory)
        audio_data = r.record(source)
        # recognize (convert from speech to text)
        alltext = r.recognize_google(audio_data,language="fr-FR",show_all=True)
        print(alltext)
        litext = set()
        for text in alltext["alternative"]:
            litext.add(GetNumFromText(text["transcript"]))
    num = set()
    for d in list(litext):
        if len(d)==6:
            num.add(d)
    print(litext)
    def myfunc(e):
        return int(e)
    if num:
        num = list(num)[0]
    else:
        sli  = list(litext)
        sli.sort(reverse=True,key=myfunc)
        num = sli[0]
    os.remove(dst)
    if len(num)==6:
        os.remove(filename)
    else :
        try:
            os.rename(filename,f"{text}-({num})"+".mp3")
        except:
            os.remove(filename)
    return num