"""
setuptools for amp

python setup.py bdist_wheel

"""

from setuptools import setup

entry_points = {
    "console_scripts": [
        "amp_server = amp.cli.amp_server:start_server",
    ]
}

setup(
    name="amp",
    version="0.0.1",
    packages=[
        "amp/broker",
        "amp/cli",
        "amp/common",
        "amp/consumer",
        "amp/producer",
        "amp/queue",
    ],
    include_package_data=True,
    install_requires=["click==8.0.3", "wheel==0.37.0"],
    entry_points=entry_points,
    platforms="any",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows"
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Build Tools",
    ],
)
