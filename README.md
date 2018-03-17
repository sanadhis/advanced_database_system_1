# Advanced Database System_I
Project I of [Advanced Database System (CS-422)](http://isa.epfl.ch/imoniteur_ISAP/!itffichecours.htm?ww_i_matiere=1888826574&ww_x_anneeAcad=2017-2018&ww_i_section=249847&ww_i_niveau=&ww_c_langue=en) of [EPFL](https://www.epfl.ch/)

## Implementation of the following data layouts:
1. Row (NSM).
2. Columnar (DSM).
3. Partition Across Data Layout (PAX).

## Implementation of the following execution models:
1. Volcano-style, tuple-at-a-time engine.
2. Column-at-a-time engine.
3. Volcano-style, vector-at-a-time engine.

## Usage

### Prerequisites
1. Install [apache maven](https://maven.apache.org/)
2. Export maven:
```bash
export PATH=$PATH:/path/to/maven/bin
```
3. Clone this repository.
4. Go inside to directory:
```bash
cd advanced_database_system_1
```

### Testing All Implementations
```bash
mvn test
```

### Example of Testing Specific Implementation
```bash
mvn -Dtest=ColumnarVectorTest test
```

## License
MIT.