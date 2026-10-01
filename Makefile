 NAME_SERVER := server
 NAME_CLIENT := client

 CXX		:= c++
 CXXFLAGS	:= -std=c++17 -Wall -Wextra -Wshadow -pthread -Iinclude
 LDFLAGS	:= -pthread
 LDLIBS		:= -lrt

 OBJ_DIR	:= obj

 SERVER_SRCS := src/server/main.cpp \
				src/server/Server.cpp \
				src/server/Topic.cpp \
				src/server/ClientRegistry.cpp \
				src/server/PrefixIndex.cpp

CLIENT_SRCS	:= $(wildcard src/client/*.cpp)

SERVER_OBJS	:= $(SERVER_SRCS:%.cpp=$(OBJ_DIR)/%.o)
CLIENT_OBJS := $(CLIENT_SRCS:%.cpp=$(OBJ_DIR)/%.o)
ALL_OBJS	:= $(SERVER_OBJS) $(CLIENT_OBJS)
DEPS		:= $(ALL_OBJS:.o=.d)

.PHONY: all clean re test

all: $(NAME_SERVER) $(if $(CLIENT_SRCS),$(NAME_CLIENT))

$(NAME_SERVER): $(SERVER_OBJS)
	$(CXX) $(SERVER_OBJS) -o $@ $(LDFLAGS) $(LDLIBS)

$(NAME_CLIENT): $(CLIENT_OBJS)
	$(CXX) $(CLIENT_OBJS) -o $@ $(LDFLAGS) $(LDLIBS)

$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

-include $(DEPS)

clean:
	rm -rf $(OBJ_DIR) $(NAME_SERVER) $(NAME_CLIENT)

re: clean all

test:
	@echo "No tests wired yet."