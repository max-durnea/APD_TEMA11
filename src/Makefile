LIB_DIR = lib
MAIN_CLASS = Tema1

# Classpath pentru bibliotecile Jackson
CLASSPATH = $(LIB_DIR)/jackson-databind-2.15.2.jar:$(LIB_DIR)/jackson-core-2.15.2.jar:$(LIB_DIR)/jackson-annotations-2.15.2.jar

# Compilare - .class rămân în directorul curent (src/)
build:
	javac -cp .:$(CLASSPATH) *.java

# Rulare - execută din directorul curent
# Fișierele .txt vor fi create aici (în src/)
run:
	java -cp .:$(CLASSPATH) $(MAIN_CLASS) $(ARGS)

# Curățare - șterge .class și .txt din src/
clean:
	rm -f *.class
	rm -f *.txt


.PHONY: build run clean
