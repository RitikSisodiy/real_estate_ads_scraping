from webdriver_manager.chrome import ChromeDriverManager

def getChromePath():
    try:
        with open("chromepath.txt",'r') as file:
            path = file.read()
        return path
    except:
        chromePath = ChromeDriverManager().install()
        with open("chromepath.txt",'w') as file:
            file.write(chromePath)
        return chromePath