/**
 * Benchmark queries from BlockSci paper using libirmin C bindings.
 *
 * This is a C port of bench/benchmark.ml, implementing the same queries
 * against the irmin-blocksci store.
 *
 * Output format: CSV with columns Query,Time_ms,Result
 *
 * Build:
 *   cd ~/caml/irmin-blocksci/c_bin && make benchmark
 *
 * Run (store must be beneath cwd due to Eio sandbox):
 *   cp -r /tmp/irmin-blocksci-store ./local-store
 *   ./benchmark ./local-store
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdbool.h>
#include "irmin.h"

/* Simple JSON value extraction (for int64 values) */
static int64_t json_get_int64(const char *json, const char *key) {
    char search[256];
    snprintf(search, sizeof(search), "\"%s\":", key);
    const char *pos = strstr(json, search);
    if (!pos) return 0;
    pos += strlen(search);
    while (*pos == ' ') pos++;
    return strtoll(pos, NULL, 10);
}

static int json_get_int(const char *json, const char *key) {
    return (int)json_get_int64(json, key);
}

/* Timing utility */
static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

/* Global store references */
static IrminRepo *repo = NULL;
static Irmin *store = NULL;

/* Create path from string */
static IrminPath *make_path(const char *path_str) {
    return irmin_path_of_string(repo, (char *)path_str, strlen(path_str));
}

/* Get string content at path (caller must free result) */
static char *get_content(const char *path_str) {
    IrminPath *path = make_path(path_str);
    if (!path) return NULL;

    IrminContents *contents = irmin_find(store, path);
    irmin_path_free(path);
    if (!contents) return NULL;

    IrminString *value = irmin_contents_to_string(repo, contents);
    irmin_contents_free(contents);
    if (!value) return NULL;

    char *result = strdup(irmin_string_data(value));
    irmin_string_free(value);
    return result;
}

/* List keys under a path */
static IrminPathArray *list_path(const char *path_str) {
    IrminPath *path = make_path(path_str);
    if (!path) return NULL;

    IrminPathArray *arr = irmin_list(store, path);
    irmin_path_free(path);
    return arr;
}

/* Get path as string (caller must free result) */
static char *path_to_string(IrminPath *path) {
    IrminType *path_type = irmin_type_path(repo);
    IrminString *str = irmin_value_to_string(path_type, (IrminValue *)path);
    irmin_type_free(path_type);
    if (!str) return NULL;

    char *result = strdup(irmin_string_data(str));
    irmin_string_free(str);
    return result;
}

/* Find last block height by scanning block keys */
static int find_last_block_height(void) {
    IrminPathArray *blocks = list_path("block");
    if (!blocks) return -1;

    uint64_t count = irmin_path_array_length(repo, blocks);
    int max_height = -1;

    for (uint64_t i = 0; i < count; i++) {
        IrminPath *p = irmin_path_array_get(repo, blocks, i);
        if (p) {
            char *pstr = path_to_string(p);
            if (pstr) {
                /* Path is like "block/123" - extract the number */
                const char *slash = strrchr(pstr, '/');
                if (slash) {
                    int height = atoi(slash + 1);
                    if (height > max_height) max_height = height;
                }
                free(pstr);
            }
            irmin_path_free(p);
        }
    }

    irmin_path_array_free(blocks);
    return max_height;
}

/* ========================================================================= */
/* Benchmark queries                                                         */
/* ========================================================================= */

/* Block count */
static int64_t query_block_count(void) {
    IrminPathArray *blocks = list_path("block");
    if (!blocks) return 0;
    int64_t count = (int64_t)irmin_path_array_length(repo, blocks);
    irmin_path_array_free(blocks);
    return count;
}

/* Tx count */
static int64_t query_tx_count(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;
    int64_t count = (int64_t)irmin_path_array_length(repo, txs);
    irmin_path_array_free(txs);
    return count;
}

/* Address count */
static int64_t query_address_count(void) {
    IrminPathArray *addrs = list_path("address");
    if (!addrs) return 0;
    int64_t count = (int64_t)irmin_path_array_length(repo, addrs);
    irmin_path_array_free(addrs);
    return count;
}

/* Input count - count all entries under index/tx_inputs */
static int64_t query_input_count(void) {
    int last_height = find_last_block_height();
    if (last_height < 0) return 0;

    int64_t count = 0;

    /* Iterate through blocks to get transactions */
    for (int height = 0; height <= last_height; height++) {
        char path[256];
        snprintf(path, sizeof(path), "index/block_txs/%d", height);

        IrminPathArray *tx_refs = list_path(path);
        if (!tx_refs) continue;

        uint64_t num_txs = irmin_path_array_length(repo, tx_refs);
        for (uint64_t i = 0; i < num_txs; i++) {
            IrminPath *tx_path = irmin_path_array_get(repo, tx_refs, i);
            if (!tx_path) continue;

            char *tx_path_str = path_to_string(tx_path);
            irmin_path_free(tx_path);
            if (!tx_path_str) continue;

            /* Get tx_id from the tx_ref content */
            char *tx_ref = get_content(tx_path_str);
            free(tx_path_str);
            if (!tx_ref) continue;

            int tx_id = json_get_int(tx_ref, "tx_id");
            free(tx_ref);

            /* Count inputs for this transaction */
            char inputs_path[256];
            snprintf(inputs_path, sizeof(inputs_path), "index/tx_inputs/%d", tx_id);

            IrminPathArray *inputs = list_path(inputs_path);
            if (inputs) {
                count += (int64_t)irmin_path_array_length(repo, inputs);
                irmin_path_array_free(inputs);
            }
        }
        irmin_path_array_free(tx_refs);
    }

    return count;
}

/* Output count - count all entries under index/tx_outputs */
static int64_t query_output_count(void) {
    int last_height = find_last_block_height();
    if (last_height < 0) return 0;

    int64_t count = 0;

    for (int height = 0; height <= last_height; height++) {
        char path[256];
        snprintf(path, sizeof(path), "index/block_txs/%d", height);

        IrminPathArray *tx_refs = list_path(path);
        if (!tx_refs) continue;

        uint64_t num_txs = irmin_path_array_length(repo, tx_refs);
        for (uint64_t i = 0; i < num_txs; i++) {
            IrminPath *tx_path = irmin_path_array_get(repo, tx_refs, i);
            if (!tx_path) continue;

            char *tx_path_str = path_to_string(tx_path);
            irmin_path_free(tx_path);
            if (!tx_path_str) continue;

            char *tx_ref = get_content(tx_path_str);
            free(tx_path_str);
            if (!tx_ref) continue;

            int tx_id = json_get_int(tx_ref, "tx_id");
            free(tx_ref);

            char outputs_path[256];
            snprintf(outputs_path, sizeof(outputs_path), "index/tx_outputs/%d", tx_id);

            IrminPathArray *outputs = list_path(outputs_path);
            if (outputs) {
                count += (int64_t)irmin_path_array_length(repo, outputs);
                irmin_path_array_free(outputs);
            }
        }
        irmin_path_array_free(tx_refs);
    }

    return count;
}

/* Tx locktime > 0 */
static int64_t query_tx_locktime_gt_0(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;

    int64_t count = 0;
    uint64_t num_txs = irmin_path_array_length(repo, txs);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, txs, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        char *tx_json = get_content(tx_path_str);
        free(tx_path_str);
        if (!tx_json) continue;

        int64_t locktime = json_get_int64(tx_json, "locktime");
        if (locktime > 0) count++;
        free(tx_json);
    }

    irmin_path_array_free(txs);
    return count;
}

/* Tx version > 1 */
static int64_t query_tx_version_gt_1(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;

    int64_t count = 0;
    uint64_t num_txs = irmin_path_array_length(repo, txs);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, txs, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        char *tx_json = get_content(tx_path_str);
        free(tx_path_str);
        if (!tx_json) continue;

        int version = json_get_int(tx_json, "version");
        if (version > 1) count++;
        free(tx_json);
    }

    irmin_path_array_free(txs);
    return count;
}

/* Max output value */
static int64_t query_max_output_value(void) {
    int last_height = find_last_block_height();
    if (last_height < 0) return 0;

    int64_t max_val = 0;

    for (int height = 0; height <= last_height; height++) {
        char path[256];
        snprintf(path, sizeof(path), "index/block_txs/%d", height);

        IrminPathArray *tx_refs = list_path(path);
        if (!tx_refs) continue;

        uint64_t num_txs = irmin_path_array_length(repo, tx_refs);
        for (uint64_t i = 0; i < num_txs; i++) {
            IrminPath *tx_path = irmin_path_array_get(repo, tx_refs, i);
            if (!tx_path) continue;

            char *tx_path_str = path_to_string(tx_path);
            irmin_path_free(tx_path);
            if (!tx_path_str) continue;

            char *tx_ref = get_content(tx_path_str);
            free(tx_path_str);
            if (!tx_ref) continue;

            int tx_id = json_get_int(tx_ref, "tx_id");
            free(tx_ref);

            char outputs_path[256];
            snprintf(outputs_path, sizeof(outputs_path), "index/tx_outputs/%d", tx_id);

            IrminPathArray *outputs = list_path(outputs_path);
            if (!outputs) continue;

            uint64_t num_outputs = irmin_path_array_length(repo, outputs);
            for (uint64_t j = 0; j < num_outputs; j++) {
                IrminPath *out_path = irmin_path_array_get(repo, outputs, j);
                if (!out_path) continue;

                char *out_path_str = path_to_string(out_path);
                irmin_path_free(out_path);
                if (!out_path_str) continue;

                /* Get output ref to find actual output */
                char *out_ref = get_content(out_path_str);
                free(out_path_str);
                if (!out_ref) continue;

                int out_tx_id = json_get_int(out_ref, "tx_id");
                int out_vout = json_get_int(out_ref, "vout");
                free(out_ref);

                char output_path[256];
                snprintf(output_path, sizeof(output_path), "output/%d/%d", out_tx_id, out_vout);

                char *output_json = get_content(output_path);
                if (output_json) {
                    int64_t value = json_get_int64(output_json, "value");
                    if (value > max_val) max_val = value;
                    free(output_json);
                }
            }
            irmin_path_array_free(outputs);
        }
        irmin_path_array_free(tx_refs);
    }

    return max_val;
}

/* Calculate fee (max fee) */
static int64_t query_calculate_fee(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;

    int64_t max_fee = 0;
    uint64_t num_txs = irmin_path_array_length(repo, txs);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, txs, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        char *tx_json = get_content(tx_path_str);
        free(tx_path_str);
        if (!tx_json) continue;

        int64_t fee = json_get_int64(tx_json, "fee");
        if (fee > max_fee) max_fee = fee;
        free(tx_json);
    }

    irmin_path_array_free(txs);
    return max_fee;
}

/* Total output value */
static int64_t query_total_output_value(void) {
    int last_height = find_last_block_height();
    if (last_height < 0) return 0;

    int64_t total = 0;

    for (int height = 0; height <= last_height; height++) {
        char path[256];
        snprintf(path, sizeof(path), "index/block_txs/%d", height);

        IrminPathArray *tx_refs = list_path(path);
        if (!tx_refs) continue;

        uint64_t num_txs = irmin_path_array_length(repo, tx_refs);
        for (uint64_t i = 0; i < num_txs; i++) {
            IrminPath *tx_path = irmin_path_array_get(repo, tx_refs, i);
            if (!tx_path) continue;

            char *tx_path_str = path_to_string(tx_path);
            irmin_path_free(tx_path);
            if (!tx_path_str) continue;

            char *tx_ref = get_content(tx_path_str);
            free(tx_path_str);
            if (!tx_ref) continue;

            int tx_id = json_get_int(tx_ref, "tx_id");
            free(tx_ref);

            char outputs_path[256];
            snprintf(outputs_path, sizeof(outputs_path), "index/tx_outputs/%d", tx_id);

            IrminPathArray *outputs = list_path(outputs_path);
            if (!outputs) continue;

            uint64_t num_outputs = irmin_path_array_length(repo, outputs);
            for (uint64_t j = 0; j < num_outputs; j++) {
                IrminPath *out_path = irmin_path_array_get(repo, outputs, j);
                if (!out_path) continue;

                char *out_path_str = path_to_string(out_path);
                irmin_path_free(out_path);
                if (!out_path_str) continue;

                char *out_ref = get_content(out_path_str);
                free(out_path_str);
                if (!out_ref) continue;

                int out_tx_id = json_get_int(out_ref, "tx_id");
                int out_vout = json_get_int(out_ref, "vout");
                free(out_ref);

                char output_path[256];
                snprintf(output_path, sizeof(output_path), "output/%d/%d", out_tx_id, out_vout);

                char *output_json = get_content(output_path);
                if (output_json) {
                    total += json_get_int64(output_json, "value");
                    free(output_json);
                }
            }
            irmin_path_array_free(outputs);
        }
        irmin_path_array_free(tx_refs);
    }

    return total;
}

/* Total fees */
static int64_t query_total_fees(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;

    int64_t total = 0;
    uint64_t num_txs = irmin_path_array_length(repo, txs);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, txs, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        char *tx_json = get_content(tx_path_str);
        free(tx_path_str);
        if (!tx_json) continue;

        total += json_get_int64(tx_json, "fee");
        free(tx_json);
    }

    irmin_path_array_free(txs);
    return total;
}

/* Avg tx per block (returns value * 1000 for precision) */
static int64_t query_avg_tx_per_block(void) {
    int64_t block_count = query_block_count();
    if (block_count == 0) return 0;

    int64_t tx_count = query_tx_count();
    return (tx_count * 1000) / block_count;
}

/* Max tx per block */
static int64_t query_max_tx_per_block(void) {
    int last_height = find_last_block_height();
    if (last_height < 0) return 0;

    int64_t max_tx = 0;

    for (int height = 0; height <= last_height; height++) {
        char path[256];
        snprintf(path, sizeof(path), "index/block_txs/%d", height);

        IrminPathArray *tx_refs = list_path(path);
        if (!tx_refs) continue;

        int64_t count = (int64_t)irmin_path_array_length(repo, tx_refs);
        if (count > max_tx) max_tx = count;
        irmin_path_array_free(tx_refs);
    }

    return max_tx;
}

/* Spent outputs */
static int64_t query_spent_outputs(void) {
    IrminPathArray *spent = list_path("index/spent_by");
    if (!spent) return 0;

    /* Count entries recursively - spent_by has tx_id/vout structure */
    int64_t count = 0;
    uint64_t num_txs = irmin_path_array_length(repo, spent);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, spent, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        IrminPathArray *vouts = list_path(tx_path_str);
        free(tx_path_str);
        if (vouts) {
            count += (int64_t)irmin_path_array_length(repo, vouts);
            irmin_path_array_free(vouts);
        }
    }

    irmin_path_array_free(spent);
    return count;
}

/* Unspent outputs */
static int64_t query_unspent_outputs(void) {
    int64_t total_outputs = query_output_count();
    int64_t spent = query_spent_outputs();
    return total_outputs - spent;
}

/* High value tx (fee > 10 BTC = 1,000,000,000 satoshis) */
static int64_t query_high_value_tx(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;

    int64_t count = 0;
    const int64_t threshold = 1000000000LL;
    uint64_t num_txs = irmin_path_array_length(repo, txs);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, txs, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        char *tx_json = get_content(tx_path_str);
        free(tx_path_str);
        if (!tx_json) continue;

        int64_t fee = json_get_int64(tx_json, "fee");
        if (fee > threshold) count++;
        free(tx_json);
    }

    irmin_path_array_free(txs);
    return count;
}

/* Multi-input tx (> 10 inputs) */
static int64_t query_multi_input_tx(void) {
    IrminPathArray *txs = list_path("tx");
    if (!txs) return 0;

    int64_t count = 0;
    uint64_t num_txs = irmin_path_array_length(repo, txs);

    for (uint64_t i = 0; i < num_txs; i++) {
        IrminPath *tx_path = irmin_path_array_get(repo, txs, i);
        if (!tx_path) continue;

        char *tx_path_str = path_to_string(tx_path);
        irmin_path_free(tx_path);
        if (!tx_path_str) continue;

        char *tx_json = get_content(tx_path_str);
        free(tx_path_str);
        if (!tx_json) continue;

        int tx_id = json_get_int(tx_json, "tx_id");
        free(tx_json);

        char inputs_path[256];
        snprintf(inputs_path, sizeof(inputs_path), "index/tx_inputs/%d", tx_id);

        IrminPathArray *inputs = list_path(inputs_path);
        if (inputs) {
            if (irmin_path_array_length(repo, inputs) > 10) count++;
            irmin_path_array_free(inputs);
        }
    }

    irmin_path_array_free(txs);
    return count;
}

/* ========================================================================= */
/* Benchmark runner                                                          */
/* ========================================================================= */

typedef struct {
    const char *name;
    int64_t (*query)(void);
} benchmark_t;

static benchmark_t benchmarks[] = {
    {"Block count", query_block_count},
    {"Tx count", query_tx_count},
    {"Input count", query_input_count},
    {"Output count", query_output_count},
    {"Address count", query_address_count},
    {"Tx locktime > 0", query_tx_locktime_gt_0},
    {"Max output value", query_max_output_value},
    {"Calculate fee", query_calculate_fee},
    {"Total output value", query_total_output_value},
    {"Total fees", query_total_fees},
    {"Tx version > 1", query_tx_version_gt_1},
    {"Avg tx per block", query_avg_tx_per_block},
    {"Max tx per block", query_max_tx_per_block},
    {"Spent outputs", query_spent_outputs},
    {"Unspent outputs", query_unspent_outputs},
    {"High value tx", query_high_value_tx},
    {"Multi-input tx", query_multi_input_tx},
    {NULL, NULL}
};

int main(int argc, char *argv[]) {
    const char *store_path = (argc > 1) ? argv[1] : "./local-store";

    /* Create config for pack store with string contents */
    IrminConfig *config = irmin_config_pack(NULL, "string");
    if (!config) {
        fprintf(stderr, "Error: Failed to create config\n");
        return 1;
    }

    /* Set the store root path */
    if (!irmin_config_set_root(config, store_path)) {
        fprintf(stderr, "Error: Failed to set root path\n");
        irmin_config_free(config);
        return 1;
    }

    /* Create repository */
    repo = irmin_repo_new(config);
    if (!repo) {
        fprintf(stderr, "Error: Failed to create repo\n");
        irmin_config_free(config);
        return 1;
    }

    if (irmin_repo_has_error(repo)) {
        IrminString *err = irmin_repo_get_error(repo);
        fprintf(stderr, "Error: %s\n", irmin_string_data(err));
        irmin_string_free(err);
        irmin_repo_free(repo);
        irmin_config_free(config);
        return 1;
    }

    /* Get main store */
    store = irmin_main(repo);
    if (!store) {
        fprintf(stderr, "Error: Failed to get main store\n");
        irmin_repo_free(repo);
        irmin_config_free(config);
        return 1;
    }

    /* Print CSV header */
    printf("Query,Time_ms,Result\n");

    /* Run benchmarks */
    for (int i = 0; benchmarks[i].name != NULL; i++) {
        double start = get_time_ms();
        int64_t result = benchmarks[i].query();
        double elapsed = get_time_ms() - start;

        printf("%s,%.3f,%ld\n", benchmarks[i].name, elapsed, (long)result);
        fflush(stdout);
    }

    /* Cleanup */
    irmin_free(store);
    irmin_repo_free(repo);
    irmin_config_free(config);

    return 0;
}
