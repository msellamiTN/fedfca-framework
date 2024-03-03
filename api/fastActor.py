from pykka import ThreadingActor
import networkx as nx

class FastActor(ThreadingActor):
    def __init__(self, obj, attr, aMat):
        super().__init__()
        self.obj = obj
        self.attr = attr
        self.aMat = aMat

    def on_receive(self, message):
        if message.get('command') == 'run_algorithm':
            result = self.run_algorithm()
            message.get('reply_to').tell({'result': result})
        else:
            print("Unknown command received.")

    def get_bipartite_cliques(self):
        cList = []
        aLen = len(self.aMat)
        bLen = len(self.aMat[0])

        dictBC = {}
        for x in range(0, aLen):
            tmpList = []
            tmpObj = [self.obj[x]]

            for y in range(0, bLen):
                if self.aMat[x][y] == 1:
                    tmpList.append(self.attr[y])

            tmp = tmpObj, tmpList
            dictBC[self.obj[x]] = tmpList
            cList.append(tmp)

        for x in range(0, bLen):
            tmpList = []
            tmpAttr = [self.attr[x]]

            for y in range(0, aLen):
                if self.aMat[y][x] == 1:
                    tmpList.append(self.obj[y])

            tmp = tmpList, tmpAttr
            dictBC[self.attr[x]] = tmpList
            cList.append(tmp)

        return cList

    def condense_list(self, inputlist):
        clist = []
        to_skip = []

        for x in range(0, len(inputlist)):
            if x in to_skip:
                continue
            matched = 0
            for y in range(x+1, len(inputlist)):
                if y in to_skip:
                    continue
                if set(inputlist[x][0]) == set(inputlist[y][0]):
                    tmp_tuple = inputlist[x][0], list(set(inputlist[x][1]).union(set(inputlist[y][1])))
                    clist.append(tmp_tuple)
                    to_skip.append(y)
                    matched = 1
                    break
                elif set(inputlist[x][1]) == set(inputlist[y][1]):
                    tmp_tuple = list(set(inputlist[x][0]).union(set(inputlist[y][0]))), inputlist[x][1]
                    clist.append(tmp_tuple)
                    to_skip.append(y)
                    matched = 1
                    break
            if matched == 0:
                clist.append(inputlist[x])

        return clist

    def supprimer_non_ferme(self, clist):
        flist = []
        listo = []
        lista = []

        for x in range(0, len(clist)):
            lista = []
            listo = []
            for y in range(0, len(clist[x][0])):
                if lista == []:
                    lista = dictBC[clist[x][0][y]]
                else:
                    lista = list(set(lista).intersection(set(dictBC[clist[x][0][y]])))

            for z in range(0, len(clist[x][1])):
                if listo == []:
                    listo = dictBC[clist[x][1][z]]
                else:
                    listo = list(set(listo).intersection(set(dictBC[clist[x][1][z]])))

            if set(lista) == set(clist[x][1]) and set(listo) == set(clist[x][0]):
                flist.append(clist[x])
        return flist

    def generate_lattice(self, bCList):
        G = nx.Graph()
        for concept in bCList:
            extent, intent = concept
            node_name = "(" + ", ".join(str(m) for m in extent) + "), (" + ", ".join(str(m) for m in intent) + ")"
            G.add_node(node_name)

        for i in range(len(bCList)):
            for j in range(i + 1, len(bCList)):
                if set(bCList[i][0]).issubset(set(bCList[j][0])) or set(bCList[j][0]).issubset(set(bCList[i][0])):
                    node_name1 = "(" + ", ".join(str(m) for m in bCList[i][0]) + "), (" + ", ".join(str(m) for m in bCList[i][1]) + ")"
                    node_name2 = "(" + ", ".join(str(m) for m in bCList[j][0]) + "), (" + ", ".join(str(m) for m in bCList[j][1]) + ")"
                    G.add_edge(node_name1, node_name2)

        pos = nx.spring_layout(G)

    def run_algorithm(self):
        dictBC = {}
        bCliques = self.get_bipartite_cliques()
        bCliquesStore = bCliques

        bCListSize = len(bCliques)
        bCListSizeCondensed = -1

        while bCListSize != bCListSizeCondensed:
            bCListSize = len(bCliques)
            bCliques = self.condense_list(bCliques)
            bCListSizeCondensed = len(bCliques)

        supremum_attrs = [at for at in self.attr if all(self.aMat[self.obj.index(o)][self.attr.index(at)] for o in self.obj)]
        supremum = (tuple(self.obj), tuple(supremum_attrs))  # Convert to tuple
        infimum_objs = [o for o in self.obj if all(self.aMat[self.obj.index(o)][self.attr.index(at)] for at in self.attr)]
        infimum = (tuple(infimum_objs), tuple(self.attr))
        if supremum not in bCliques:
            bCliques.append(supremum)
        if infimum not in bCliques:
            bCliques.append(infimum)
        bCliques = list(set(tuple(tuple(x) for x in sub) for sub in bCliques))

        bCliques.sort(key=lambda x: len(x[0]), reverse=True)

        conceptDict = {}
        for x in range(len(bCliques)):
            obj_str = "".join(str(m) for m in sorted(bCliques[x][0]))
            attr_str = "".join(str(m) for m in sorted(bCliques[x][1]))
            conceptDict[obj_str] = set(bCliques[x][1])
            conceptDict[attr_str] = set(bCliques[x][0])
        self.generate_lattice(bCliques)
        return bCliques
