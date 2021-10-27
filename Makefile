context=-f devops/docker-compose.yml

run:
	docker-compose $(context) up -d

stop:
	docker-compose $(context) stop

clear:
	docker-compose $(context) rm

clear_all: clear
	docker volume prune