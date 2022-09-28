import java.util.Random;

public final class RandomHelper {

    private static final Random RANDOM_GENERATOR = new Random(300);

    private RandomHelper() {

    }

    public static int randomInRange(int lowerBound, int upperBound) {
        Random rand = new Random(nextInt());
        return rand.nextInt(upperBound-lowerBound) + lowerBound;
    }

    public static int nextInt() {
        return RANDOM_GENERATOR.nextInt();
    }

}
