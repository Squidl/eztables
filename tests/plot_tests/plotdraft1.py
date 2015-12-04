import eztables
import plottables

table=eztables.eztable("draft1.arff")
print(table)
plot=plottables.scatter(table,{"xs":"lex_rich","ys":"semis_word_mean","c":"author"},{})
plot.show()
table.save("draft1.csv")
