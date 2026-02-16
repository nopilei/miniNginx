import contextvars

client_addr_var: contextvars.ContextVar[str] = contextvars.ContextVar('client_addr_var', default="-")