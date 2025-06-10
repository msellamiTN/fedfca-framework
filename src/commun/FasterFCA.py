class FasterFCA:
    def __init__(self, threshold=0.0, context_sensitivity=0.0,privacy=False):
        self.ARCHBIT = (sys.maxsize.bit_length() + 1)  # Dynamically calculate architecture word size
        self.BIT = 1 << (self.ARCHBIT - 1)
        self.formal_context = {}
        self.objects = 0
        self.attributes = 0
        self.context = []
        self.dictBC = {}
        self.lattice = []
        self.threshold = threshold
        self.context_sensitivity = context_sensitivity
        self.context_sensitivity=context_sensitivity
        self.privacy=privacy
        self.StabilityCalculator = StabilityCalculator(self.formal_context)
        self.LDPHandler = LDPHandler(self.formal_context, self.context_sensitivity)
    def extract_formal_context(self, file):
        """
        Extract the formal context from a file with the structure:
        1: {2, 3, 5}
        2: {4, 5, 6}
        """
        self.formal_context = {}

        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                obj_id_str, attributes_str = line.split(":")
                obj_id = int(obj_id_str.strip())
                attributes = ast.literal_eval(attributes_str.strip())

                if not isinstance(attributes, set):
                    raise ValueError("Attributes must be a set.")

                self.formal_context[obj_id] = {int(attr) for attr in attributes}

            except ValueError as ve:
                print(f"Value error in line: '{line}' - {ve}")
            except SyntaxError as se:
                print(f"Syntax error in line: '{line}' - {se}")
            except Exception as e:
                print(f"Unexpected error in line: '{line}' - {e}")

        return self.formal_context

    def read_context(self, formal_context):
        """Process the context from the formal context dictionary using bitwise operations."""
        self.objects = len(formal_context)
        self.attributes = max(attr for attrs in formal_context.values() for attr in attrs)

        context_width = (self.attributes // self.ARCHBIT) + 1
        self.context = [[0] * context_width for _ in range(self.objects)]

        object_to_index = {obj_id: idx for idx, obj_id in enumerate(formal_context.keys())}

        for obj_id, attributes in formal_context.items():
            obj_index = object_to_index[obj_id]
            for attribute in attributes:
                col = (attribute - 1) // self.ARCHBIT
                bit_position = self.ARCHBIT - 1 - ((attribute - 1) % self.ARCHBIT)
                self.context[obj_index][col] |= (1 << bit_position)

    def get_bipartite_cliques(self):
        """Generate bipartite cliques from the formal context using bitset representation."""
        cList = []
        for i in range(self.objects):
            tmpList = []
            tmpObj = [i + 1]
            for j in range(self.attributes):
                if self.context[i][j // self.ARCHBIT] & (1 << (self.ARCHBIT - 1 - (j % self.ARCHBIT))):
                    tmpList.append(j + 1)

            cList.append((tmpObj, tmpList))

        for j in range(self.attributes):
            tmpList = []
            tmpAttr = [j + 1]
            for i in range(self.objects):
                if self.context[i][j // self.ARCHBIT] & (1 << (self.ARCHBIT - 1 - (j % self.ARCHBIT))):
                    tmpList.append(i + 1)

            cList.append((tmpList, tmpAttr))

        return cList

    def condense_list(self, inputlist):
        logging.debug("Initial input list: %s", inputlist)
        clist = []
        to_skip = []

        for x in range(len(inputlist)):
            if x in to_skip:
                continue
            matched = 0
            for y in range(x + 1, len(inputlist)):
                if y in to_skip:
                    continue
                if set(inputlist[x][0]) == set(inputlist[y][0]):
                    tmp_tuple = inputlist[x][0], list(set(inputlist[x][1]).union(set(inputlist[y][1])))
                    logging.debug("Merging on extent: %s and %s", inputlist[x], inputlist[y])
                    clist.append(tmp_tuple)
                    to_skip.append(y)
                    matched = 1
                    break
                elif set(inputlist[x][1]) == set(inputlist[y][1]):
                    tmp_tuple = list(set(inputlist[x][0]).union(set(inputlist[y][0]))), inputlist[x][1]
                    logging.debug("Merging on intent: %s and %s", inputlist[x], inputlist[y])
                    clist.append(tmp_tuple)
                    to_skip.append(y)
                    matched = 1
                    break
            if matched == 0:
                clist.append(inputlist[x])

        logging.debug("Condensed list: %s", clist)
        return clist

    def generate_lattice(self, bCList):
        """Generate the lattice structure."""
        G = nx.DiGraph()  # Using DiGraph to capture hierarchy
        nodes = []

        for concept in bCList:
            extent, intent = concept
            node_name = f"({', '.join(map(str, extent))}), ({', '.join(map(str, intent))})"
            G.add_node(node_name)
            nodes.append((extent, intent, node_name))

        for i, (e1, i1, n1) in enumerate(nodes):
            for j, (e2, i2, n2) in enumerate(nodes):
                if i != j and set(e1).issubset(e2) and set(i2).issubset(i1):
                    G.add_edge(n2, n1)

        pos = nx.spring_layout(G, seed=42)  # Reproducible layout
        nx.draw(G, pos, with_labels=True, node_size=5000, node_color="skyblue")
        nx.write_graphml(G, "lattice.graphml")  # Export to .graphml for external analysis

        return G


    def save_concepts_to_file(self, concepts, output_file):
        """
        Save the computed concepts to a file in JSON format.
        """

        lattice_dict = {}
        for i, (concept, stability) in enumerate(concepts, start=1):
            extent, intent = concept
            intent_str = [str(attr) for attr in intent]
            extent_str = [str(obj) for obj in extent]
            concept_dict = {"Intent": intent_str, "Extent": extent_str, "Stability":stability}

            lattice_dict[f"Concept {i}"] = concept_dict

        with open(output_file, 'w') as f:
            json.dump(lattice_dict, f, indent=4)
    def convert_bcliques_to_lattice(self, bcliques):
        """
        Convert bipartite cliques to lattice format with hashable structures.

        :param bcliques: List of bipartite cliques, each as a tuple (extent, intent).
        :return: Lattice as a list of tuples (frozenset, frozenset).
        """
        lattice = []
        for extent, intent in bcliques:
            # Convert extent and intent to frozensets
            extent_fset = frozenset(extent)
            intent_fset = frozenset(intent)
            lattice.append((extent_fset, intent_fset))
        return lattice



    def stablity_compute(self):
        """
        Compute the stability values for the lattice and filter based on a threshold.

        :param threshold: Minimum stability value required for a concept to be included.
        :return: A tuple containing the filtered lattice with stability values and average stability.
        """
        filtered_lattice_with_stability = []
        total_stability = 0
        count = 0

        for i, concept in enumerate(self.lattice, start=1):
            extent, intent = concept

            stability_value = self.StabilityCalculator.compute_stability_for_concept(extent, intent)
            #print(f"Concept {i}: {concept}, {extent}: {intent},{stability_value}")

            if stability_value >= self.threshold:
                filtered_lattice_with_stability.append(((extent, intent), stability_value))
                #print(f"Concept {i}: {concept}, Stability: {stability_value}")
                total_stability += stability_value
                count += 1

        average_stability = total_stability / count if count > 0 else 0
        return filtered_lattice_with_stability, average_stability

    def run(self, input_file, output_file):
        """
        Execute the Faster FCA algorithm to compute formal concepts and save the result,
        filtering the lattice based on a stability threshold.

        :param input_file: Path to the input file containing the formal context.
        :param output_file: Path to the output file to save results.
        :param threshold: Minimum stability value required for a concept to be included.
        :return: Filtered lattice with stability values in the format [((frozenset, frozenset), stability_value)].
        """
        # Step 1: Read formal context from the input file
        try:
            with open(input_file, 'r') as f:
                self.formal_context = self.extract_formal_context(f)
                #print(self.formal_context)
        except Exception as e:
            raise ValueError(f"Error reading input file: {e}")

        # Step 2: Process the formal context into internal representation
        # Apply LDP to formal context before generating bipartite cliques
        if self.privacy :
           self.LDPHandler.formal_context = self.formal_context
           self.ldp_context=self.LDPHandler.apply_aldp_to_formal_context()
           self.read_context(self.ldp_context)
        else :
          self.read_context(self.formal_context)


        #print(self.compare_formal_contexts())
        # Step 3: Generate bipartite cliques
        bCliques = self.get_bipartite_cliques()

        # Step 4: Condense bipartite cliques iteratively
        bCliquesSize = len(bCliques)
        while True:
            bCliquesCondensed = self.condense_list(bCliques)
            if len(bCliquesCondensed) == bCliquesSize:
                break
            bCliquesSize = len(bCliquesCondensed)
            bCliques = bCliquesCondensed

        # Step 5: Convert bipartite cliques into lattice format
        self.lattice = self.convert_bcliques_to_lattice(bCliques)

        # Debug: Display intermediate results
        #print("Formal Context:", self.formal_context)
        #print("Bipartite Cliques (Lattice):", self.lattice)

        # Step 6: Assign formal context and lattice to StabilityCalculator
        self.StabilityCalculator.formal_context = self.formal_context
        self.StabilityCalculator.lattice = self.lattice

        # Step 7: Compute stability and filter based on threshold
        filtered_lattice_with_stability, average_stability = self.stablity_compute()
        #print(f"Filtered Lattice (Threshold: {self.threshold}):", filtered_lattice_with_stability)
        # print("Average Stability:", average_stability)

        # Step 8: Save the filtered lattice to the output file
        self.save_concepts_to_file(filtered_lattice_with_stability, output_file)

        # Return the filtered lattice in a format compatible with Aggregator
        #print("differences",self.compare_formal_contexts())
        return average_stability,filtered_lattice_with_stability
    def compare_formal_contexts(self):
        """
        Compare the original formal context with the noisy formal context
        and output the differences for each object.

        :param original_context: Original formal context (before LDP).
        :param noisy_context: Noisy formal context (after LDP).
        :return: A dictionary with differences for each object.
        """
        differences = {}

        # Iterate through the objects in the original context
        for obj_index, original_attributes in self.formal_context.items():
            # Get the noisy attributes for the object (default to empty set if not in noisy context)
            noisy_attributes = self.ldp_context.get(obj_index, set())

            # Find attributes that were added or removed
            added_attributes = noisy_attributes - original_attributes
            removed_attributes = original_attributes - noisy_attributes

            # Store the differences
            if added_attributes or removed_attributes:
                differences[obj_index] = {
                    'added': sorted(list(added_attributes)),
                    'removed': sorted(list(removed_attributes))
                }
        #print("Origianal",self.formal_context)
        #print('Noizy',self.ldp_context)
        return differences

