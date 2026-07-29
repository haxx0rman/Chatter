"""
ChatterCore - A self-contained, general-purpose real-time communication module
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="chattercore",
    version="2.0.0",
    author="Michael Diaz",
    author_email="michael@diaz.dev",
    description="A self-contained, general-purpose real-time communication module",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/haxx0rman/Chatter",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Communications",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "websockets>=11.0.0",
        "pydantic>=2.0.0",
        "typing-extensions>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio",
            "black",
            "flake8",
            "mypy",
            "coverage",
        ],
    },
    entry_points={
        "console_scripts": [
            "chattercore-server=chattercore.server:main",
            "chattercore-client=chattercore.client:main",
        ],
    },
)
