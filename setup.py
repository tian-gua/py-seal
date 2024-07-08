import setuptools

setuptools.setup(
    name='py-seal',  # 应用名
    version='0.2.4',  # 版本号
    description="一个python快速web开发框架",  # 描述
    author="melon",  # 作者
    packages=setuptools.find_packages(),  # 包括在安装包内的 Python 包 ，尽量和name一致及模块名
)
