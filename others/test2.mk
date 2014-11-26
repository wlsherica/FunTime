OBJS = a.out b.out c.out
INSTDIR = /home/erica_li/FunTime/others

build: ${OBJS}
#	@if[	-d ${INSTDIR}	]; \
#		then \ 
#		touch ${OBJS} \
#		echo "${OBJS} finished" \
#	else \
#		echo "Oh, ${OBJS} does not exist..."; \
#	fi
rebuild: clean build
	@echo "Rebuild done"
a.out: b.out
	touch $@
	@echo "$@ exists"
b.out: c.out
	touch $@
	@echo "$@ exists"
c.out:
	touch $@
	@echo "$@ exists"
clean:
	@rm -rf *.out
