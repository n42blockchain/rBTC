// Test-only adapter to the unmodified Bitcoin Core 31.0 Linearize template.
// Build instructions and pinned source hashes are in the cluster acceptance report.
#include <cluster_linearize.h>
#include <util/bitset.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>

// Preserve upstream assertion failures without linking the complete node's
// version-reporting implementation. No algorithm or arithmetic is replaced.
void assertion_fail(const std::source_location& location, std::string_view expression)
{
    std::cerr << location.file_name() << ':' << location.line() << ": " << expression << '\n';
    std::abort();
}

int main(int argc, char** argv)
{
    if (argc != 2) return 64;
    std::ifstream input{argv[1]};
    unsigned cases;
    if (!(input >> cases) || cases > 20000) return 65;
    std::cout << "RBTC_CORE31_LINEARIZE_V1\n";
    for (unsigned testcase = 0; testcase < cases; ++testcase) {
        uint32_t count, old_mode;
        uint64_t budget, seed;
        if (!(input >> count >> budget >> seed >> old_mode) || count > 64 || old_mode > 2) return 65;
        cluster_linearize::DepGraph<BitSet<64>> graph;
        std::vector<BitSet<64>> parents(count);
        int64_t total_fee{0};
        int32_t total_size{0};
        for (uint32_t child = 0; child < count; ++child) {
            int64_t fee;
            int32_t size;
            unsigned num_parents;
            if (!(input >> fee >> size >> num_parents) || fee < 0 || size <= 0 || num_parents > count) return 65;
            if (fee > std::numeric_limits<int64_t>::max() - total_fee || size > std::numeric_limits<int32_t>::max() - total_size) return 65;
            total_fee += fee;
            total_size += size;
            graph.AddTransaction(FeeFrac{fee, size});
            for (unsigned edge = 0; edge < num_parents; ++edge) {
                uint32_t parent;
                if (!(input >> parent) || parent >= count || parent == child) return 65;
                parents[child].Set(parent);
            }
        }
        std::vector<cluster_linearize::DepGraphIndex> order(count), positions(count, count);
        for (uint32_t pos = 0; pos < count; ++pos) {
            if (!(input >> order[pos]) || order[pos] >= count || positions[order[pos]] != count) return 65;
            positions[order[pos]] = pos;
        }
        for (uint32_t child = 0; child < count; ++child) {
            if (old_mode == 1) for (auto parent : parents[child]) if (positions[parent] >= positions[child]) return 65;
            graph.AddDependencies(parents[child], child);
        }
        auto [result, optimal, cost] = cluster_linearize::Linearize(
            graph, budget, seed, cluster_linearize::IndexTxOrder{},
            old_mode ? std::span<const uint32_t>{order} : std::span<const uint32_t>{}, old_mode != 2);
        std::cout << optimal << ' ' << cost << ' ' << count;
        order = std::move(result);
        for (auto index : order) std::cout << ' ' << index;
        std::cout << '\n';
    }
    std::string trailing;
    if (input >> trailing) return 65;
}
