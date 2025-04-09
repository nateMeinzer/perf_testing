
CC=gcc
CFLAGS=-O3 -lm -I.
OBJS=dsdgen.o build.o driver.o rnd.o print.o permute.o text.o address.o date.o dist.o s_web_site.o s_web_page.o s_catalog_page.o s_call_center.o s_store.o s_warehouse.o s_customer_address.o s_customer.o s_customer_demographics.o s_household_demographics.o s_income_band.o s_item.o s_promotion.o s_reason.o s_ship_mode.o s_time.o s_date.o s_supplier.o s_web_sales.o s_web_returns.o s_catalog_sales.o s_catalog_returns.o s_store_sales.o s_store_returns.o s_inventory.o

all: dsdgen

dsdgen: $(OBJS)
	$(CC) $(CFLAGS) -o dsdgen $(OBJS)

clean:
	rm -f *.o dsdgen
