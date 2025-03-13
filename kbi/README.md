In order to re-buid the kernel:

`just apply`

Will update the running ipykernel, which should be run off of this virtual env, e.g.,:

```
poetry shell
jupyter-notebook --autoreload --no-browser
```