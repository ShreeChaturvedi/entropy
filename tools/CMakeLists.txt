# ─────────────────────────────────────────────────────────────────────────────
# Entropy Database Engine - Tools
# ─────────────────────────────────────────────────────────────────────────────

# entropy-sim: deterministic crash-simulation CLI runner
add_executable(entropy_sim_cli
    entropy-sim/main.cpp
)

target_link_libraries(entropy_sim_cli
    PRIVATE
        entropy_sim
)

set_target_properties(entropy_sim_cli PROPERTIES
    OUTPUT_NAME "entropy-sim"
)
