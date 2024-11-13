---
title: Installation
---
# Installation

You can install this package using either [`pipx`](https://pypa.github.io/pipx/)
or `pip`. Installing via `pipx` offers advantages if you want to just use the
`adtl` tool standalone from the command line, as it isolates the Python
package dependencies in a virtual environment. On the other hand, `pip` installs
packages to the global environment which is generally not recommended as it
can interfere with other packages on your system.

* **Installation via `pipx`** (recommended):

  ```shell
  pipx install adtl
  ```

* **Installation via `pip`**:

  ```shell
  python3 -m pip install adtl
  ```

If you are writing code which depends on adtl (instead of using
the command-line program), then it is best to add a dependency on
`git+https://github.com/globaldothealth/adtl` to your Python build tool of
choice.
