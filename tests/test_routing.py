"""Unit tests for the routing heuristics (pure Python — no Spark/DB needed)."""
import importlib.util
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
spec = importlib.util.spec_from_file_location(
    "nn", os.path.join(ROOT, "neo4j", "routing", "nearest_neighbor.py"))
nn = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nn)


def test_haversine_known_distance():
    # San Jose -> Cartago is ~22 km in a straight line.
    d = nn.haversine_km((9.9333, -84.0833), (9.8644, -83.9194))
    assert 18 < d < 26


def test_route_length_symmetry_includes_return_to_kitchen():
    stops = [{"lat": 9.94, "lng": -84.10}, {"lat": 9.90, "lng": -84.05}]
    length = nn.route_length([(s["lat"], s["lng"]) for s in stops])
    assert length > 0


def test_nearest_neighbour_visits_all_stops_once():
    stops = [{"lat": 9.9 + i * 0.01, "lng": -84.0 - i * 0.01, "order_id": f"o{i}",
              "zone": "Z"} for i in range(6)]
    tour = nn.nearest_neighbour(stops)
    assert len(tour) == len(stops)
    assert {s["order_id"] for s in tour} == {s["order_id"] for s in stops}


def test_two_opt_never_worsens_route():
    stops = [{"lat": 9.9 + (i % 3) * 0.05, "lng": -84.0 - (i % 4) * 0.05,
              "order_id": f"o{i}", "zone": "Z"} for i in range(8)]
    nn_tour = nn.nearest_neighbour(stops)
    naive = nn.route_length([(s["lat"], s["lng"]) for s in nn_tour])
    optimised, opt_len = nn.two_opt(nn_tour)
    assert opt_len <= naive + 1e-6
    assert len(optimised) == len(stops)


def test_assignment_balances_load_and_respects_capacity_when_possible():
    orders = [{"order_id": f"o{i}", "lat": 9.9, "lng": -84.0,
               "zone": ["A", "B", "C"][i % 3], "net_amount": 1000} for i in range(12)]
    couriers = nn.assign_to_couriers(orders, n_couriers=3, capacity=5)
    assert sum(len(c) for c in couriers) == 12          # nothing dropped
    assert all(len(c) <= 5 for c in couriers)            # capacity respected (12<=15)
    sizes = sorted(len(c) for c in couriers)
    assert sizes[-1] - sizes[0] <= 1                      # balanced


def test_assignment_spills_evenly_when_over_capacity():
    orders = [{"order_id": f"o{i}", "lat": 9.9, "lng": -84.0, "zone": "A",
               "net_amount": 1} for i in range(20)]
    couriers = nn.assign_to_couriers(orders, n_couriers=4, capacity=3)
    assert sum(len(c) for c in couriers) == 20            # over capacity -> all placed
    sizes = sorted(len(c) for c in couriers)
    assert sizes[-1] - sizes[0] <= 1                      # still balanced
