# app/models.py
import os
from sqlalchemy import (
    Column, Integer, BigInteger, String, DateTime, Date, Numeric,
    ForeignKey, SmallInteger, Boolean, Text, Index
)
from sqlalchemy.orm import relationship
from .database import Base

# ============================================================
#  Configuración de esquema
# ============================================================
DB_SCHEMA = os.getenv("DB_SCHEMA", "agente_virtual")

# ============================================================
#  Catálogos / entidades base
# ============================================================
class TamanioEmpresa(Base):
    __tablename__  = "tamanio_empresa"
    __table_args__ = {"schema": DB_SCHEMA}

    id_tamanio_empresa = Column(SmallInteger, primary_key=True)
    codigo = Column(String(16))
    nombre = Column(String(64))

    entidades = relationship("Entidad", back_populates="tamanio", lazy="selectin")


class Entidad(Base):
    __tablename__  = "entidad"
    __table_args__ = {"schema": DB_SCHEMA}

    id_entidad        = Column(Integer, primary_key=True)
    tipo_persona      = Column(String)                   # ENUM(...) si aplica en DB
    identificacion    = Column(String(32), index=True)
    nombre_legal      = Column(String(200))
    nombre_comercial  = Column(String(200))
    email             = Column(String(200))
    telefono          = Column(String(32))
    id_tamanio_empresa= Column(
        SmallInteger,
        ForeignKey(f"{DB_SCHEMA}.tamanio_empresa.id_tamanio_empresa"),
        index=True
    )
    direccion         = Column(String(255))

    tamanio = relationship("TamanioEmpresa", back_populates="entidades", lazy="selectin")

    # Relaciones inversas útiles (no estrictamente necesarias):
    # - como cliente/proveedor en facturas:
    facturas_cxc_cliente = relationship(
        "FacturaCXC",
        foreign_keys="FacturaCXC.id_entidad_cliente",
        viewonly=True,
        lazy="selectin"
    )
    facturas_cxc_vendedor = relationship(
        "FacturaCXC",
        foreign_keys="FacturaCXC.id_entidad_vendedor",
        viewonly=True,
        lazy="selectin"
    )
    facturas_cxp_proveedor = relationship(
        "FacturaCXP",
        foreign_keys="FacturaCXP.id_entidad_proveedor",
        viewonly=True,
        lazy="selectin"
    )


class Moneda(Base):
    __tablename__  = "moneda"
    __table_args__ = {"schema": DB_SCHEMA}

    id_moneda = Column(Integer, primary_key=True)
    codigo    = Column(String(8), unique=True, index=True)
    nombre    = Column(String(64))


class PuntoVenta(Base):
    __tablename__  = "punto_venta"
    __table_args__ = {"schema": DB_SCHEMA}

    id_punto_venta = Column(Integer, primary_key=True)
    codigo         = Column(Integer, index=True)
    descripcion    = Column(String(120))


class RolEntidad(Base):
    __tablename__  = "rol_entidad"
    __table_args__ = {"schema": DB_SCHEMA}

    id_rol_entidad = Column(Integer, primary_key=True)
    id_entidad     = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    rol            = Column(String)       # ENUM(...) si aplica en DB
    activo         = Column(Boolean)
    creado_en      = Column(DateTime)

    entidad = relationship("Entidad", lazy="selectin")


class Contacto(Base):
    __tablename__  = "contacto"
    __table_args__ = {"schema": DB_SCHEMA}

    id_contacto = Column(Integer, primary_key=True)
    id_entidad  = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    nombre      = Column(String(120))
    apellido    = Column(String(120))
    cargo       = Column(String(120))
    telefono    = Column(String(32))
    email       = Column(String(200))
    es_principal= Column(Boolean)         # TINYINT(1) en MySQL, BOOLEAN en PG

    entidad = relationship("Entidad", lazy="selectin")

# Índices recomendados
Index("ix_entidad_identificacion", Entidad.identificacion)
Index("ix_puntoventa_codigo", PuntoVenta.codigo)

# ============================================================
#  Cuentas por Cobrar (CxC)
# ============================================================
class FacturaCXC(Base):
    __tablename__  = "factura_cxc"
    __table_args__ = {"schema": DB_SCHEMA}

    id_cxc         = Column(Integer, primary_key=True)
    numero_factura = Column(String(64), nullable=False, index=True)
    fecha_emision  = Column(DateTime, nullable=False, index=True)
    fecha_pago     = Column(DateTime)                         # puede ser NULL
    fecha_limite   = Column(DateTime, nullable=False, index=True)
    dias_credito   = Column(Integer)
    monto          = Column(Numeric(14, 2), nullable=False)
    monto_pagado   = Column(Numeric(14, 2), nullable=False, default=0)
    pagada         = Column(Boolean, default=False)
    observaciones  = Column(Text)

    id_entidad_cliente  = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    id_entidad_vendedor = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    id_punto_venta      = Column(Integer, ForeignKey(f"{DB_SCHEMA}.punto_venta.id_punto_venta"), index=True)
    id_moneda           = Column(Integer, ForeignKey(f"{DB_SCHEMA}.moneda.id_moneda"), index=True)

    cliente     = relationship("Entidad", foreign_keys=[id_entidad_cliente],  lazy="selectin")
    vendedor    = relationship("Entidad", foreign_keys=[id_entidad_vendedor], lazy="selectin")
    punto_venta = relationship("PuntoVenta", lazy="selectin")
    moneda      = relationship("Moneda", lazy="selectin")

    detalles = relationship(
        "DetalleCXC",
        back_populates="factura",
        cascade="all, delete-orphan",
        lazy="selectin"
    )
    pagos = relationship(
        "PagoCXC",
        back_populates="factura",
        cascade="all, delete-orphan",
        lazy="selectin"
    )


class DetalleCXC(Base):
    __tablename__  = "detalle_cxc"
    __table_args__ = {"schema": DB_SCHEMA}

    id_detalle_cxc = Column(Integer, primary_key=True)
    id_cxc         = Column(Integer, ForeignKey(f"{DB_SCHEMA}.factura_cxc.id_cxc"), index=True)
    descripcion    = Column(String(200))
    cantidad       = Column(Numeric(12, 2))
    precio_unitario= Column(Numeric(12, 2))
    impuesto       = Column(Numeric(12, 2))
    total_linea    = Column(Numeric(12, 2))

    factura = relationship("FacturaCXC", back_populates="detalles", lazy="selectin")


class PagoCXC(Base):
    __tablename__  = "pago_cxc"
    __table_args__ = {"schema": DB_SCHEMA}

    id_pago_cxc = Column(Integer, primary_key=True)
    id_cxc      = Column(Integer, ForeignKey(f"{DB_SCHEMA}.factura_cxc.id_cxc"), index=True)
    fecha       = Column(DateTime, index=True)
    monto       = Column(Numeric(12, 2))
    metodo      = Column(String(32))
    referencia  = Column(String(64))
    id_moneda   = Column(Integer, ForeignKey(f"{DB_SCHEMA}.moneda.id_moneda"), index=True)

    factura = relationship("FacturaCXC", back_populates="pagos", lazy="selectin")
    moneda  = relationship("Moneda", lazy="selectin")

# ============================================================
#  Cuentas por Pagar (CxP)
# ============================================================
class FacturaCXP(Base):
    __tablename__  = "factura_cxp"
    __table_args__ = {"schema": DB_SCHEMA}

    id_cxp         = Column(Integer, primary_key=True)
    numero_factura = Column(String(64), nullable=False, index=True)
    fecha_emision  = Column(DateTime, nullable=False, index=True)
    fecha_pago     = Column(DateTime)                         # puede ser NULL
    fecha_limite   = Column(DateTime, nullable=False, index=True)
    dias_compra    = Column(Integer)
    monto          = Column(Numeric(14, 2), nullable=False)
    monto_pagado   = Column(Numeric(14, 2), nullable=False, default=0)
    pagada         = Column(Boolean, default=False)
    observaciones  = Column(Text)

    id_entidad_proveedor = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    id_punto_venta       = Column(Integer, ForeignKey(f"{DB_SCHEMA}.punto_venta.id_punto_venta"), index=True)
    id_moneda            = Column(Integer, ForeignKey(f"{DB_SCHEMA}.moneda.id_moneda"), index=True)

    proveedor  = relationship("Entidad",     foreign_keys=[id_entidad_proveedor], lazy="selectin")
    punto_venta= relationship("PuntoVenta",  lazy="selectin")
    moneda     = relationship("Moneda",      lazy="selectin")

    detalles = relationship(
        "DetalleCXP",
        back_populates="factura",
        cascade="all, delete-orphan",
        lazy="selectin"
    )
    pagos = relationship(
        "PagoCXP",
        back_populates="factura",
        cascade="all, delete-orphan",
        lazy="selectin"
    )


class DetalleCXP(Base):
    __tablename__  = "detalle_cxp"
    __table_args__ = {"schema": DB_SCHEMA}

    id_detalle_cxp = Column(Integer, primary_key=True)
    id_cxp         = Column(Integer, ForeignKey(f"{DB_SCHEMA}.factura_cxp.id_cxp"), index=True)
    descripcion    = Column(String(200))
    cantidad       = Column(Numeric(12, 2))
    precio_unitario= Column(Numeric(12, 2))
    impuesto       = Column(Numeric(12, 2))
    total_linea    = Column(Numeric(12, 2))

    factura = relationship("FacturaCXP", back_populates="detalles", lazy="selectin")


class PagoCXP(Base):
    __tablename__  = "pago_cxp"
    __table_args__ = {"schema": DB_SCHEMA}

    id_pago_cxp = Column(Integer, primary_key=True)
    id_cxp      = Column(Integer, ForeignKey(f"{DB_SCHEMA}.factura_cxp.id_cxp"), index=True)
    fecha       = Column(DateTime, index=True)
    monto       = Column(Numeric(12, 2))
    metodo      = Column(String(32))
    referencia  = Column(String(64))
    id_moneda   = Column(Integer, ForeignKey(f"{DB_SCHEMA}.moneda.id_moneda"), index=True)

    factura = relationship("FacturaCXP", back_populates="pagos", lazy="selectin")
    moneda  = relationship("Moneda", lazy="selectin")

# ============================================================
#  Alertas / configuración de crédito
# ============================================================
class ConfigAlertaCredito(Base):
    __tablename__  = "config_alerta_credito"
    __table_args__ = {"schema": DB_SCHEMA}

    id_config          = Column(Integer, primary_key=True)
    alcance            = Column(String)   # ENUM(...) si aplica
    id_tamanio_empresa = Column(
        SmallInteger,
        ForeignKey(f"{DB_SCHEMA}.tamanio_empresa.id_tamanio_empresa"),
        index=True
    )
    id_entidad         = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    umbral_monto       = Column(Numeric(12, 2))
    regla_dias_atraso  = Column(Integer)
    mensaje            = Column(String(200))
    prioridad          = Column(SmallInteger)
    activo             = Column(Boolean)
    vigencia_desde     = Column(Date)
    vigencia_hasta     = Column(Date)

    entidad = relationship("Entidad", lazy="selectin")
    tamanio = relationship("TamanioEmpresa", lazy="selectin")


class AlertaEvento(Base):
    __tablename__  = "alerta_evento"
    __table_args__ = {"schema": DB_SCHEMA}

    id_alerta_evento   = Column(BigInteger, primary_key=True)
    id_config          = Column(Integer, ForeignKey(f"{DB_SCHEMA}.config_alerta_credito.id_config"), index=True)
    id_entidad         = Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    id_entidad_vendedor= Column(Integer, ForeignKey(f"{DB_SCHEMA}.entidad.id_entidad"), index=True)
    id_factura_cxc     = Column(Integer, ForeignKey(f"{DB_SCHEMA}.factura_cxc.id_cxc"), index=True)
    operador_texto     = Column(String(120))
    motivo             = Column(String(200))
    decision           = Column(String)     # ENUM(...) si aplica
    observacion        = Column(Text)
    creado_en          = Column(DateTime, index=True)

    config      = relationship("ConfigAlertaCredito", lazy="selectin")
    entidad     = relationship("Entidad", foreign_keys=[id_entidad], lazy="selectin")
    vendedor    = relationship("Entidad", foreign_keys=[id_entidad_vendedor], lazy="selectin")
    factura_cxc = relationship("FacturaCXC", lazy="selectin")
