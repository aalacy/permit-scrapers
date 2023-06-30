from flask import Flask, request, make_response
from flask_restful import Resource, Api
from selenium import webdriver
from selenium.webdriver import Firefox, Chrome, ChromeOptions, FirefoxProfile
from selenium.webdriver.firefox.options import Options as FirefoxOptions
import os

app = Flask(__name__)
api = Api(app)

class Selenium(Resource):
    _driver = None
    basedir = os.path.abspath(os.path.dirname(__file__))

    @staticmethod
    def getDriver():
        if not Selenium._driver:
            options = FirefoxOptions()
            profile = FirefoxProfile()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            # options.add_argument('--headless')
            profile.set_preference("permissions.default.image", 2)
            path = f"{Selenium.basedir}/data/geckodriver.exe"
            Selenium._driver = Firefox(executable_path=path, options=options, firefox_profile=profile)

        return Selenium._driver

    @property
    def driver(self):
        return Selenium.getDriver()

    def get(self):
        url = str(request.args['url'])

        self.driver.get(url)

        return make_response(self.driver.page_source)

api.add_resource(Selenium, '/')

if __name__ == '__main__':
    app.run(debug=True)