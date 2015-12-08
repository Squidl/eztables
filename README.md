# eztables
Easy python table manipulation. The idea of this project is to make a simple python module that can easily load files in different table formats (.csv, arff, xlt), do generic table transformations/processing, and save to multiple different formats (.csv, .arff, xlt, etc... but also latex, html or string patterned formats). Ideally the module would remain simple enough to be used by CLI with almost all features.

## Modules
A quick overview of the modules included in this project.

### eztables
A simple table module.

### tables_cli
A cli interface for the eztables module.

### plottables
A simple wrapper for matplotlib plots using eztable as input.

### plot_cli
A cli interface for the plottables module.

### iotable
A module that defines an interface to load and save tables. Can be extended for most table applications.

## Table formats
Currently this module only covers a limited subset of formats.
| Input and Output  | Output only |
| ------------- | ------------- |
| csv  | pprint  |
| arff  | LaTeX  |
Coming soon:
| Input and Output |
| ---------------- |
| Google Sheets |
| xlt |
| SQL Servers |
