# AMQP Utilities

# do a search whether a particular given routing_key matches a given topic 
# bind pattern such as topic.*.something.#
# Attempt to reject non-matching messages as early as possible to save cycles
def match_routing_key(routing_key, bind_pattern):
 
    if routing_key == bind_pattern:
        return True # direct match

    routing_key_elements = routing_key.split(".")
    num_routing_key_elements = len(routing_key_elements)
    
    bind_pattern_elements = bind_pattern.split(".")
    num_bind_pattern_elements = len(bind_pattern_elements)

    # start with the length of the elements and last element first as this 
    # could be an easy reject
    # print("routing_key=%s matches bind_pattern=%s" % (routing_key, bind_pattern))
    if (num_routing_key_elements <= num_bind_pattern_elements) or (bind_pattern_elements[-1:][0] == "#"):
        for i in range(num_routing_key_elements):
            if bind_pattern_elements[i] == "#":
                return True # final match
            elif (bind_pattern_elements[i] == "*") or (routing_key_elements[i] == bind_pattern_elements[i]):
                # this element matches pattern; check if we are at the end of the routing_key elements
                if i == num_routing_key_elements - 1:
                    return True # final match; last element matches
            elif i == num_routing_key_elements - 1:
                # we are at the end of the elements without a match so far
                # account for the scenario where we are at the end of the routing_key elements
                # but there is a # is present in the search pattern on the next element
                if (num_routing_key_elements + 1 == num_bind_pattern_elements): 
                    # there is one more element in pattern
                    #logger.debug("there is exactly one more element in the pattern")
                    if routing_key_elements[i] == bind_pattern_elements[i+1]:
                        return True # 
            else:
                # bail out as we do not have a match on this element
                return False
    
    # routing_key does not match bind_pattern
    return False

# strip supplied prefix from a routing key if it is present otherwise return
# routing_key unharmed
# account for whether a delimiter (.) may or may not be present at the end
def remove_routing_key_prefix(routing_key, prefix):
    if not prefix.endswith("."): prefix += "."
    if routing_key.startswith(prefix):
        return routing_key[len(prefix):]
    return routing_key # no change