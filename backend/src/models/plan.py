from sqlalchemy import String, JSON, Double, ForeignKey, Boolean, Integer, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class AnalysisPlan(Base):
    __tablename__ = 'analysis_plans'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    definition: Mapped[dict] = mapped_column(JSON, default=dict)
    output_step: Mapped[str] = mapped_column(String(40), default='')
    version: Mapped[int] = mapped_column(Integer, default=1)
    created_by: Mapped[int] = mapped_column(ForeignKey('users.id'))
    created_at: Mapped[float] = mapped_column(Double)
    updated_at: Mapped[float] = mapped_column(Double)


class PlanRun(Base):
    __tablename__ = 'plan_runs'
    id: Mapped[int] = mapped_column(primary_key=True)
    plan_id: Mapped[int] = mapped_column(ForeignKey('analysis_plans.id'))
    plan_name: Mapped[str] = mapped_column(String(160))
    plan_version: Mapped[int] = mapped_column(Integer)
    definition: Mapped[dict] = mapped_column(JSON, default=dict)
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    dataset_checksum: Mapped[str] = mapped_column(String(64))
    output_step: Mapped[str] = mapped_column(String(40), default='')
    status: Mapped[str] = mapped_column(String(20), default='queued')
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    approved_by: Mapped[str] = mapped_column(String(150), default='')
    approved_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    error: Mapped[str] = mapped_column(String(1000), default='')
    created_by: Mapped[int] = mapped_column(ForeignKey('users.id'))
    created_at: Mapped[float] = mapped_column(Double)
    started_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)


class PlanStepRun(Base):
    __tablename__ = 'plan_step_runs'
    __table_args__ = (
        UniqueConstraint('run_id', 'step_key', name='uq_plan_step_run_key'),
        Index('ix_plan_step_runs_status', 'status'),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey('plan_runs.id'))
    step_key: Mapped[str] = mapped_column(String(40))
    name: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(20), default='queued')
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    fingerprint: Mapped[str] = mapped_column(String(64), default='')
    definition: Mapped[dict] = mapped_column(JSON, default=dict)
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    input_count: Mapped[int] = mapped_column(Integer, default=0)
    input_batch_ids: Mapped[list] = mapped_column(JSON, default=list)
    provenance: Mapped[dict] = mapped_column(JSON, default=dict)
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    selection_signature: Mapped[str] = mapped_column(String(64), default='')
    error: Mapped[str] = mapped_column(String(1000), default='')
    reused_from_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
