from setuptools import setup, find_packages

setup(
    name="pg-helpers",
    version="1.0.0",
    author="Chris Leonard",
    description="PostgreSQL helper functions for data analysis",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "psycopg2-binary>=2.9.0",
        "sqlalchemy>=1.4.0",
        "python-dotenv>=0.19.0",
    ],
    extras_require={
        "windows": ["winsound"],  # Windows-specific sound support
    },
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7+",
    ],
)
