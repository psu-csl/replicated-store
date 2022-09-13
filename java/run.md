### Running java version of replicant

- to compile it from the source you should have the `maven`
- once you get and install it, call `mvn compile`
- to run,
  call `mvn exec:java -Dexec.args="0 /home/vjabrayilov/research/misopt/replicated-store/c++/config.json"
  `
    - 0 is id
    - the path is to `config.json`