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

NAME_TEST		:= test_runner
TEST_SRCS		:= $(wildcard tests/*.cpp)
TEST_OBJS		:= $(TEST_SRCS:%.cpp=$(OBJ_DIR)/%.o)
TEST_DATA_STRUCT_OBJS	:= $(OBJ_DIR)/src/server/PrefixIndex.o

GTEST_CXXFLAGS	:= $(shell pkg-config --cflags gtest gtest_main 2>/dev/null)
GTEST_LDLIBS	:= $(shell pkg-config --libs gtest gtest_main 2>/dev/null || echo -lgtest -lgtest_main)

DEPS		:= $(ALL_OBJS:.o=.d) $(TEST_OBJS:.o=.d)

.PHONY: all clean re test

all: $(NAME_SERVER) $(if $(CLIENT_SRCS),$(NAME_CLIENT))

$(NAME_SERVER): $(SERVER_OBJS)
	$(CXX) $(SERVER_OBJS) -o $@ $(LDFLAGS) $(LDLIBS)

$(NAME_CLIENT): $(CLIENT_OBJS)
	$(CXX) $(CLIENT_OBJS) -o $@ $(LDFLAGS) $(LDLIBS)

$(OBJ_DIR)/tests/%.o: tests/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(GTEST_CXXFLAGS) -MMD -MP -c $< -o $@

$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

-include $(DEPS)

clean:
	rm -rf $(OBJ_DIR) $(NAME_SERVER) $(NAME_CLIENT) $(NAME_TEST)

re: clean all

ifeq ($(TEST_SRCS),)
test:
	@echo "No tests found in tests/"
else
test: $(NAME_TEST)
	./$(NAME_TEST)

$(NAME_TEST): $(TEST_OBJS) $(TEST_DATA_STRUCT_OBJS)
	$(CXX) $(TEST_OBJS) $(TEST_DATA_STRUCT_OBJS) -o $@ $(LDFLAGS) $(GTEST_LDLIBS) $(LDLIBS)
endif