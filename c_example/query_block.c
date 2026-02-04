/**
 * Proof of concept: Query the first block from irmin-blocksci store using libirmin C bindings.
 *
 * Build:
 *   # First, build libirmin from the irmin-eio repository:
 *   cd ~/caml/irmin-eio && dune build src/libirmin/lib/
 *
 *   # Then compile this example:
 *   gcc -o query_block query_block.c \
 *       -I ~/caml/irmin-eio/_build/default/src/libirmin/lib \
 *       -L ~/caml/irmin-eio/_build/default/src/libirmin/lib \
 *       -lirmin -lasmrun -lunix -lcamlstr
 *
 * Run:
 *   LD_LIBRARY_PATH=~/caml/irmin-eio/_build/default/src/libirmin/lib ./query_block
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "irmin.h"

int main(int argc, char *argv[]) {
    /* Use store path from command line or default */
    const char *store_path = (argc > 1) ? argv[1] : "/tmp/irmin-blocksci-store";

    printf("=== irmin-blocksci C Query Example ===\n\n");

    /* Create config for pack store with string contents */
    printf("1. Creating pack store config...\n");
    IrminConfig *config = irmin_config_pack(NULL, "string");
    if (!config) {
        fprintf(stderr, "Error: Failed to create config\n");
        return 1;
    }

    /* Set the store root path */
    printf("2. Setting store root to: %s\n", store_path);
    if (!irmin_config_set_root(config, store_path)) {
        fprintf(stderr, "Error: Failed to set root path\n");
        irmin_config_free(config);
        return 1;
    }

    /* Create repository */
    printf("3. Opening repository...\n");
    IrminRepo *repo = irmin_repo_new(config);
    if (!repo) {
        fprintf(stderr, "Error: Failed to create repo\n");
        irmin_config_free(config);
        return 1;
    }

    /* Check for errors */
    if (irmin_repo_has_error(repo)) {
        IrminString *err = irmin_repo_get_error(repo);
        fprintf(stderr, "Error: %s\n", irmin_string_data(err));
        irmin_string_free(err);
        irmin_repo_free(repo);
        irmin_config_free(config);
        return 1;
    }

    /* Get main store (branch) */
    printf("4. Getting main branch...\n");
    Irmin *store = irmin_main(repo);
    if (!store) {
        fprintf(stderr, "Error: Failed to get main store\n");
        irmin_repo_free(repo);
        irmin_config_free(config);
        return 1;
    }

    /* Create path for block/0 (the genesis block) */
    printf("5. Creating path for 'block/0'...\n");
    const char *path_str = "block/0";
    IrminPath *path = irmin_path_of_string(repo, (char *)path_str, strlen(path_str));
    if (!path) {
        fprintf(stderr, "Error: Failed to create path\n");
        irmin_free(store);
        irmin_repo_free(repo);
        irmin_config_free(config);
        return 1;
    }

    /* Find contents at path */
    printf("6. Looking up block 0...\n");
    IrminContents *contents = irmin_find(store, path);
    if (!contents) {
        printf("   Block 0 not found in store.\n");
        printf("   Make sure you have imported data first:\n");
        printf("   dune exec irmin-blocksci -- import <csv-export-dir>\n");
    } else {
        /* Convert contents to string */
        IrminString *value = irmin_contents_to_string(repo, contents);
        if (value) {
            printf("\n=== Block 0 (Genesis Block) ===\n");
            printf("%s\n", irmin_string_data(value));
            irmin_string_free(value);
        }
        irmin_contents_free(contents);
    }

    /* Cleanup */
    printf("\n7. Cleaning up...\n");
    irmin_path_free(path);
    irmin_free(store);
    irmin_repo_free(repo);
    irmin_config_free(config);

    printf("Done!\n");
    return 0;
}
