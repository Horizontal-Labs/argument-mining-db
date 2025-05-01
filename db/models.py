from sqlalchemy import (
    Integer,
    String,
    Integer,
    ForeignKey,
    CheckConstraint
)

from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from .base import Base 

class Domain(Base):
    __tablename__ = 'domain'
    
    id: Mapped[int] = mapped_column(primary_key=True,autoincrement="auto")
    domain_name: Mapped[str] = mapped_column(String(256), nullable=False)

    def __repr__(self): 
        return (
            f"domain_id: {self.id} "
            f"domain_name: {self.domain_name}"
        )
    


class ADU(Base):
    __tablename__ = 'adu'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement="auto")
    text: Mapped[str] = mapped_column(MEDIUMTEXT, nullable=False)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    domain_id: Mapped[int] = mapped_column(ForeignKey('domain.id'), nullable=False)
    
    __table_args__ = (
    CheckConstraint("type IN ('premise', 'claim')", name="check_type_valid"),
    )
    
    def __repr__(self): 
        return (
            f"ADU with the id {self.id} "
            f"text: {self.text} "
            f"Type: {self.type} "
            f"Domain_id: {self.domain_id}"
        )

class Relationship(Base):
    __tablename__ = 'relationship'
    
    id: Mapped[int] = mapped_column(autoincrement="auto",primary_key=True)
    from_adu_id: Mapped[int] = mapped_column(ForeignKey('adu.id'), nullable=False)
    to_adu_id: Mapped[int] = mapped_column(ForeignKey('adu.id'), nullable=False)
    category: Mapped[str] = mapped_column(String(16), nullable=False)
    domain_id: Mapped[int] = mapped_column(ForeignKey('domain.id'), nullable=False)
    
    __table_args__ = (
    CheckConstraint("category IN ('support', 'stance_pro', 'stance_con')", name="check_category_valid"),
    )
    def __repr__(self): 
        return (
            f"Id: {self.id} "
            f"From adu: {self.from_adu_id} "
            f"to_adu_id: {self.to_adu_id} "
            f"Category: {self.category} "
            f"domain_id: {self.domain_id}"
        )