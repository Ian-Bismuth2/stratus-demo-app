class UnitConverter {
    validateInput(val, unit) {
        if (val === null || val === undefined) {
            throw new Error(`Invalid value: ${val} for unit ${unit}`);
        }
        if (typeof val !== 'number' || isNaN(val)) {
            throw new Error(`Value must be a valid number, got: ${val}`);
        }
        if (unit === 'K' && val < 0) {
            throw new Error('Temperature cannot be below absolute zero (0K)');
        }
    }

    // Helper for more precise temperature conversions
    convertTemperature(val, fromUnit, toUnit, formula) {
        this.validateInput(val, fromUnit);
        // Use a more precise conversion approach
        const precise = formula(val);
        return this.round(precise, toUnit);
    }

    round(n, unit) {
        if (!unit || !(unit in this.decimalPlaces)) {
            return [n, unit];
        }
        // Use a more precise rounding approach to avoid floating point errors
        const scale = Math.pow(10, this.decimalPlaces[unit]);
        return [Number((Math.round(n * scale) / scale).toFixed(this.decimalPlaces[unit])), unit];
    }
}

export class Imperial extends UnitConverter {
    decimalPlaces = {
        'F': 0,
        'ft': 0,
        'mph': 0,
        'inHg': 2,
    };

    convert(val, unit) {
        this.validateInput(val, unit);
        
        switch (unit) {
            case 'K':
                return this.convertTemperature(val, 'K', 'F', 
                    (k) => ((k - 273.15) * 1.8) + 32);
            case 'm':
                if (val < 0) {
                    throw new Error('Length cannot be negative');
                }
                return this.round(val * 3.2808, 'ft');
            case 'm/s':
                return this.round(val * 2.237, 'mph');
            case 'Pa':
                if (val < 0) {
                    throw new Error('Pressure cannot be negative');
                }
                return this.round(val * 0.0002953, 'inHg');
            default:
                return this.round(val, unit);
        }
    }
}

export class Metric extends UnitConverter{
    decimalPlaces = {
        'C': 0,
        'm': 0,
        'm/s': 0,
        'Pa': 2,
    };

    convert(val, unit) {
        this.validateInput(val, unit);
        
        switch (unit) {
            case 'K':
                return this.convertTemperature(val, 'K', 'C', 
                    (k) => k - 273.15);
            case 'm':
                if (val < 0) {
                    throw new Error('Length cannot be negative');
                }
                return this.round(val, 'm');
            case 'm/s':
                return this.round(val, 'm/s');
            case 'Pa':
                if (val < 0) {
                    throw new Error('Pressure cannot be negative');
                }
                return this.round(val, 'Pa');
            default:
                return this.round(val, unit);
        }
    }
}