from webdriver_manager.chrome import ChromeDriverManager


def getChromePath():
    try:
        # Try to open and read the chromepath.txt file
        with open("chromepath.txt", "r") as file:
            path = file.read()
        return path
    except:
        # If the file doesn't exist or an error occurs, install ChromeDriver and save the path to chromepath.txt
        chromePath = ChromeDriverManager().install()
        with open("chromepath.txt", "w") as file:
            file.write(chromePath)
        return chromePath
