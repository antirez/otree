all: btree-example

btree-example: btree.c btree_example.c
	$(CC) -o btree_example btree.c btree_example.c -Wall -W -g -rdynamic -ggdb -O2

clean:
	rm -rf btree_example
