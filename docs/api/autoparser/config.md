# AutoParser Configuration

The settings for AutoParser (most noteably the LLM choices and API key, and schema locations)
are configured using the Config class. This can be initialised by providing either a dictionary
or JSON/TOML config file to the `setup_config` function at the top of your python file.

```{eval-rst}
.. autofunction:: adtl.autoparser.setup_config
```

## Class definitions

The various options and defaults are described here:

```{eval-rst}
.. autoclass:: adtl.autoparser.config.config.Config
    :members:
```
