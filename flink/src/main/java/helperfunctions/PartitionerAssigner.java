/*Copyright (c) 2022 Data Intensive Applications and Systems Laboratory (DIAS)
                   Ecole Polytechnique Federale de Lausanne
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

package helperfunctions;

import partitioning.*;
import partitioning.dalton.*;

enum PARTITIONING_ALG{
    SHUFFLING,
    TWO_CHOICES,
    HASHING,
    DALTON,
    CM,
    cAM,
    DAGreedy
}

public class PartitionerAssigner {

    public static Partitioner initializePartitioner(String str, int parallelism, int slide, int size, int numOfKeys) throws IllegalArgumentException{
        PARTITIONING_ALG partitioning_alg = parse(str);
        return assign(partitioning_alg, parallelism, slide, size, numOfKeys);
    }

    private static PARTITIONING_ALG parse(String str){
        PARTITIONING_ALG partitioning_alg;
        switch (str){
            case "SHUFFLING":
                partitioning_alg = PARTITIONING_ALG.SHUFFLING;
                break;
            case "TWO_CHOICES":
                partitioning_alg = PARTITIONING_ALG.TWO_CHOICES;
                break;
            case "HASHING":
                partitioning_alg = PARTITIONING_ALG.HASHING;
                break;
            case "DALTON":
                partitioning_alg = PARTITIONING_ALG.DALTON;
                break;
            case "CM":
                partitioning_alg = PARTITIONING_ALG.CM;
                break;
            case "cAM":
                partitioning_alg = PARTITIONING_ALG.cAM;
                break;
            case "DAGreedy":
                partitioning_alg = PARTITIONING_ALG.DAGreedy;
                break;
            default:
                throw new IllegalArgumentException("Unknown Partitioning Algorithm " + str);
        }
        return partitioning_alg;
    }

    private static Partitioner assign(PARTITIONING_ALG algorithm, int parallelism, int slide, int size, int numOfKeys) throws IllegalArgumentException {
        Partitioner partitioner;
        if (algorithm == PARTITIONING_ALG.SHUFFLING) {
            partitioner = new Shuffling(parallelism);
        } else if (algorithm == PARTITIONING_ALG.TWO_CHOICES) {
            partitioner = new TwoChoices(size, slide, parallelism);
        } else if (algorithm == PARTITIONING_ALG.HASHING) {
            partitioner = new Hashing(parallelism);
        } else if (algorithm == PARTITIONING_ALG.DALTON){
            partitioner = new Dalton(parallelism, slide, size, numOfKeys);
        } else if (algorithm == PARTITIONING_ALG.CM){
            partitioner = new CM(size, slide, parallelism);
        } else if (algorithm == PARTITIONING_ALG.cAM){
            partitioner = new cAM(size, slide, parallelism);
        } else if (algorithm == PARTITIONING_ALG.DAGreedy){
            partitioner = new DAGreedy(parallelism, slide, size, numOfKeys);
        } else {
            throw new IllegalArgumentException("Unknown Partitioning Algorithm" + algorithm);
        }
        return partitioner;
    }
}
