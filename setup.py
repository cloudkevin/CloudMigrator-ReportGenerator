from setuptools import setup

setup(
	name='cmReportGenerator',
	version='1.0',
	py_modules=['cmReportGenerator'],
	install_requires=['click','progress','pandas','google-api-python-client','google-auth-httplib2','google-auth-oauthlib','bs4'],
	entry_points='''
		[console_scripts]
		cmReportGenerator=cmReportGenerator:main
	'''
	)