# Introduction

This REPO contains Python code developed for Shou Group Liquid Handling Robotic to perform Evolution Experiment.

# How to compile

Make sure you have installed PyInstaller on your build environment. 

If not, run the following:
```
pip install pyinstaller
```

This utility is intended to be compiled into a standalone executable so that it can be called directly from the Hamilton/VENUS workflow without requiring the user to manually run Python.

To compile, use the following command:

```shell
pyinstaller --onefile --noconsole YourSciptName.py
```

The resulting file could be found under `dist` folder of your REPO. 

# Python module for Hamilton VENUS method

- [Champions_FL](./Champions_FL/Champions_FL.md)

# Python Code to Control Hardware

- [Teleshake](./Teleshake/Teleshake.md)

# VENUS Code Backup

- [Champions_FL_Python](./VENUS-Method/Champions_FL_Python/)
- [ChamFL_Flourscent](./VENUS-Method/ChamFL_Flourscent/)