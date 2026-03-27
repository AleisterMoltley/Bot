"""Tests for the Hourly Risk Regime module."""

from datetime import datetime
from unittest.mock import patch


class TestHourlyRiskRegime:
    """Test HourlyRiskRegime class."""

    def test_regime_initialization(self):
        """Test regime initializes with correct defaults."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        assert regime._risk_map is not None
        assert len(regime._risk_map) == 24  # 24 hours

    def test_get_multiplier_returns_valid_values(self):
        """Test get_multiplier returns only valid multiplier values."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        valid_multipliers = {0.0, 0.3, 1.6, 1.8}

        for hour in range(24):
            multiplier = regime.get_multiplier(hour)
            assert multiplier in valid_multipliers, (
                f"Invalid multiplier {multiplier} for hour {hour}"
            )

    def test_get_multiplier_matches_risk_map(self):
        """Test get_multiplier returns values from RISK_MAP."""
        from polybot.hourly_risk_regime import HourlyRiskRegime, RISK_MAP

        regime = HourlyRiskRegime()

        for hour in range(24):
            expected = RISK_MAP[hour]
            actual = regime.get_multiplier(hour)
            assert actual == expected, f"Hour {hour}: expected {expected}, got {actual}"

    def test_is_active_returns_false_for_zero_multiplier(self):
        """Test is_active returns False when multiplier is 0.0."""
        from polybot.hourly_risk_regime import HourlyRiskRegime, RISK_MAP

        regime = HourlyRiskRegime()

        for hour in range(24):
            is_active = regime.is_active(hour)
            multiplier = RISK_MAP[hour]
            if multiplier == 0.0:
                assert is_active is False, f"Hour {hour} should be inactive"
            else:
                assert is_active is True, f"Hour {hour} should be active"

    def test_inactive_hours_are_6_7_18_19(self):
        """Test that inactive hours (0.0 multiplier) are 6, 7, 18, 19 Berlin time."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        inactive_hours = [6, 7, 18, 19]

        for hour in range(24):
            if hour in inactive_hours:
                assert regime.is_active(hour) is False, (
                    f"Hour {hour} should be inactive"
                )
                assert regime.get_multiplier(hour) == 0.0
            else:
                assert regime.is_active(hour) is True, f"Hour {hour} should be active"

    def test_conservative_hours_are_correct(self):
        """Test conservative hours (0.3 multiplier) are correct."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        conservative_hours = [3, 11, 12, 13, 17, 23]

        for hour in conservative_hours:
            multiplier = regime.get_multiplier(hour)
            assert multiplier == 0.3, f"Hour {hour} should be conservative (0.3)"

    def test_aggressive_hours_are_correct(self):
        """Test aggressive hours (1.6 or 1.8 multiplier) are correct."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        aggressive_hours_16 = [0, 1, 2, 4, 5, 8, 10, 14, 16, 20, 21, 22]
        aggressive_hours_18 = [9, 15]

        for hour in aggressive_hours_16:
            multiplier = regime.get_multiplier(hour)
            assert multiplier == 1.6, f"Hour {hour} should be aggressive (1.6)"

        for hour in aggressive_hours_18:
            multiplier = regime.get_multiplier(hour)
            assert multiplier == 1.8, f"Hour {hour} should be super aggressive (1.8)"

    def test_get_risk_level_returns_correct_labels(self):
        """Test get_risk_level returns correct human-readable labels."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()

        # Inactive hour
        assert regime.get_risk_level(6) == "inactive"
        assert regime.get_risk_level(7) == "inactive"

        # Conservative hour
        assert regime.get_risk_level(3) == "conservative"
        assert regime.get_risk_level(11) == "conservative"

        # Aggressive hour
        assert regime.get_risk_level(0) == "aggressive"
        assert regime.get_risk_level(9) == "aggressive"

    def test_get_color_returns_correct_colors(self):
        """Test get_color returns correct CSS colors."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()

        # Inactive = grey
        assert regime.get_color(6) == "#6B7280"

        # Conservative = red
        assert regime.get_color(3) == "#EF4444"

        # Aggressive = green
        assert regime.get_color(0) == "#22C55E"

    def test_get_state_returns_hourly_risk_state(self):
        """Test get_state returns HourlyRiskState dataclass."""
        from polybot.hourly_risk_regime import HourlyRiskRegime, HourlyRiskState

        regime = HourlyRiskRegime()
        state = regime.get_state()

        assert isinstance(state, HourlyRiskState)
        assert hasattr(state, "berlin_hour")
        assert hasattr(state, "multiplier")
        assert hasattr(state, "risk_level")
        assert hasattr(state, "color")
        assert hasattr(state, "is_active")
        assert hasattr(state, "timestamp")

    def test_get_state_to_dict(self):
        """Test get_state().to_dict() returns correct format."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        state = regime.get_state()
        state_dict = state.to_dict()

        assert "berlin_hour" in state_dict
        assert "multiplier" in state_dict
        assert "risk_level" in state_dict
        assert "color" in state_dict
        assert "is_active" in state_dict
        assert "timestamp" in state_dict
        assert isinstance(state_dict["berlin_hour"], int)
        assert isinstance(state_dict["multiplier"], float)
        assert isinstance(state_dict["risk_level"], str)

    def test_get_heatmap_data_returns_24_hours(self):
        """Test get_heatmap_data returns data for all 24 hours."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        heatmap = regime.get_heatmap_data()

        assert len(heatmap) == 24

        for i, item in enumerate(heatmap):
            assert item["hour"] == i
            assert "hour_label" in item
            assert "multiplier" in item
            assert "risk_level" in item
            assert "color" in item
            assert "is_current" in item

    def test_get_heatmap_data_marks_current_hour(self):
        """Test get_heatmap_data marks exactly one hour as current."""
        from polybot.hourly_risk_regime import HourlyRiskRegime

        regime = HourlyRiskRegime()
        heatmap = regime.get_heatmap_data()

        current_hours = [item for item in heatmap if item["is_current"]]
        assert len(current_hours) == 1

    @patch("polybot.hourly_risk_regime.datetime")
    def test_get_berlin_hour_uses_berlin_timezone(self, mock_datetime):
        """Test get_berlin_hour uses Berlin timezone correctly."""
        from polybot.hourly_risk_regime import HourlyRiskRegime, BERLIN_TZ

        # Mock datetime.now to return a specific time in Berlin
        mock_now = datetime(2026, 3, 19, 14, 30, 0)  # 14:30 Berlin
        mock_datetime.now.return_value = mock_now

        regime = HourlyRiskRegime()
        hour = regime.get_berlin_hour()

        mock_datetime.now.assert_called_once_with(BERLIN_TZ)
        assert hour == 14


class TestGlobalFunctions:
    """Test global convenience functions."""

    def test_get_hourly_risk_regime_returns_singleton(self):
        """Test get_hourly_risk_regime returns singleton."""
        from polybot.hourly_risk_regime import get_hourly_risk_regime

        regime1 = get_hourly_risk_regime()
        regime2 = get_hourly_risk_regime()
        assert regime1 is regime2

    def test_get_hourly_multiplier_convenience_function(self):
        """Test get_hourly_multiplier convenience function."""
        from polybot.hourly_risk_regime import (
            get_hourly_multiplier,
            get_hourly_risk_regime,
        )

        # Should return same value as regime.get_multiplier()
        multiplier = get_hourly_multiplier()
        regime = get_hourly_risk_regime()
        expected = regime.get_multiplier()
        assert multiplier == expected

    def test_is_trading_active_convenience_function(self):
        """Test is_trading_active convenience function."""
        from polybot.hourly_risk_regime import is_trading_active, get_hourly_risk_regime

        # Should return same value as regime.is_active()
        is_active = is_trading_active()
        regime = get_hourly_risk_regime()
        expected = regime.is_active()
        assert is_active == expected


class TestRiskMapValues:
    """Test the RISK_MAP values are correctly configured."""

    def test_risk_map_has_24_entries(self):
        """Test RISK_MAP has exactly 24 entries."""
        from polybot.hourly_risk_regime import RISK_MAP

        assert len(RISK_MAP) == 24

    def test_risk_map_keys_are_0_to_23(self):
        """Test RISK_MAP keys are 0-23."""
        from polybot.hourly_risk_regime import RISK_MAP

        assert set(RISK_MAP.keys()) == set(range(24))

    def test_risk_map_values_are_valid(self):
        """Test RISK_MAP values are all valid multipliers."""
        from polybot.hourly_risk_regime import RISK_MAP

        valid_values = {0.0, 0.3, 1.6, 1.8}
        for hour, multiplier in RISK_MAP.items():
            assert multiplier in valid_values, (
                f"Invalid multiplier {multiplier} for hour {hour}"
            )

    def test_risk_map_et_to_berlin_mapping(self):
        """Test specific ET to Berlin mappings from problem statement."""
        from polybot.hourly_risk_regime import RISK_MAP

        # Verify the ET → Berlin conversion from the problem statement
        # 12a ET → 6a Berlin (grau)
        assert RISK_MAP[6] == 0.0
        # 3a ET → 9a Berlin (grün, best hour)
        assert RISK_MAP[9] == 1.8
        # 5a ET → 11a Berlin (rot)
        assert RISK_MAP[11] == 0.3
        # 9a ET → 15p Berlin (grün, best hour)
        assert RISK_MAP[15] == 1.8
        # 12p ET → 18p Berlin (grau)
        assert RISK_MAP[18] == 0.0


class TestRiskLabelsAndColors:
    """Test risk labels and colors configuration."""

    def test_risk_labels_mapping(self):
        """Test RISK_LABELS has correct mappings."""
        from polybot.hourly_risk_regime import RISK_LABELS

        assert RISK_LABELS[0.0] == "inactive"
        assert RISK_LABELS[0.3] == "conservative"
        assert RISK_LABELS[1.6] == "aggressive"
        assert RISK_LABELS[1.8] == "aggressive"

    def test_risk_colors_mapping(self):
        """Test RISK_COLORS has correct mappings."""
        from polybot.hourly_risk_regime import RISK_COLORS

        assert RISK_COLORS["inactive"] == "#6B7280"
        assert RISK_COLORS["conservative"] == "#EF4444"
        assert RISK_COLORS["aggressive"] == "#22C55E"
