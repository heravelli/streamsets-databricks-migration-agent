from models.migration import TeamProgress
from state.state_manager import StateManager


class ProgressTracker:
    def __init__(self, state_manager: StateManager):
        self.sm = state_manager

    def all_teams(self) -> list[TeamProgress]:
        state = self.sm.load()
        return state.all_team_progress()

    def team(self, team_name: str) -> TeamProgress:
        state = self.sm.load()
        return state.get_team_progress(team_name)

    def overall(self) -> dict:
        state = self.sm.load()
        total = len(state.pipelines)
        approved = sum(1 for r in state.pipelines.values() if r.status.value == "approved")
        return {
            "total": total,
            "approved": approved,
            "completion_pct": round(approved / total * 100, 1) if total else 0.0,
            "teams": len(state.team_index),
        }
