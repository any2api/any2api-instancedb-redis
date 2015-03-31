var chai = require('chai');
var expect = chai.expect;

var InstanceDB = require('../');

var dbConfig = {};

if (process.env.REDIS_HOST && process.env.REDIS_PORT) {
  dbConfig.redisConfig = {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT
  };
}

var db = InstanceDB(dbConfig);



describe('instances database smoke test', function() {
  describe('#set()', function() {
    it('should set new instance w/o parameters', function(done) {
      var args = {
        id: '1',
        instance: { created: '1970-01-01' },
        executableName: 'textexec'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        done();
      });
    });

    it('should set new instance w/ parameters', function(done) {
      var args = {
        id: '2',
        instance: { created: '1970-01-02', status: 'running', parameters: { foo: 'bar' } },
        invokerName: 'testinv'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        done();
      });
    });

    it('should set existing instance w/ updated parameters', function(done) {
      var args = {
        id: '2',
        instance: { created: '1970-01-02', status: 'running', parameters: { foo: 'nop' } },
        invokerName: 'testinv'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        done();
      });
    });
  });

  describe('#get()', function() {
    it('should get instance w/o parameters', function(done) {
      var args = {
        id: '11',
        instance: { created: '1970-01-01', parameters: { foo: 'bar' } },
        invokerName: 'testinv'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        db.instances.get(args, function(err, instance) {
          expect(err).to.not.exist;
          expect(instance.parameters).to.not.exist;
          expect(instance.created).to.equal('1970-01-01');

          done();
        });
      });
    });
  });

  describe('#getAll()', function() {
    it('should get all instances', function(done) {
      db.instances.getAll({ invokerName: 'testinv', status: 'running' }, function(err, instances) {
        expect(err).to.not.exist;
        expect(instances).to.not.be.empty;

        done();
      });
    });
  });

  describe('#remove()', function() {
    it('should remove given instance', function(done) {
      var args = {
        id: '21',
        instance: { created: '1970-01-01', parameters: { foo: 'bar' } },
        invokerName: 'testinv'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        db.instances.remove(args, function(err) {
          expect(err).to.not.exist;

          done();
        });
      });
    });
  });
});



describe('parameters database smoke test', function() {
  describe('#set()', function() {
    it('should set parameter of instance', function(done) {
      var args = {
        id: '101',
        instance: { created: '1970-01-01', status: 'running' },
        invokerName: 'testinv',
        parameterName: 'foo',
        value: 'bar'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        db.parameters.set(args, function(err) {
          expect(err).to.not.exist;

          db.parameters.get(args, function(err, value) {
            expect(err).to.not.exist;
            expect(value).to.equal('bar');

            done();
          });
        });
      });
    });
  });

  describe('#getAll()', function() {
    it('should get all parameters of instance', function(done) {
      var args = {
        id: '102',
        instance: { created: '1970-01-01', status: 'running', parameters: { foo: 'bar', fox: 'bay' } },
        invokerName: 'testinv'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        db.parameters.getAll(args, function(err, params) {
          expect(err).to.not.exist;
          expect(params).to.include.keys('foo', 'fox');
          expect(params.foo).to.equal('bar');
          expect(params.fox).to.equal('bay');

          done();
        });
      });
    });
  });
});



describe('results database smoke test', function() {
  describe('#set()', function() {
    it('should set result of instance', function(done) {
      var args = {
        id: '201',
        instance: { created: '1970-01-01', status: 'running' },
        invokerName: 'testinv',
        resultName: 'foo',
        value: 'bar'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        db.results.set(args, function(err) {
          expect(err).to.not.exist;

          db.results.get(args, function(err, value) {
            expect(err).to.not.exist;
            expect(value).to.equal('bar');

            done();
          });
        });
      });
    });
  });

  describe('#getAll()', function() {
    it('should get all results of instance', function(done) {
      var args = {
        id: '202',
        instance: { created: '1970-01-01', status: 'running', results: { foo: 'bar', fox: 'bay' } },
        invokerName: 'testinv'
      };

      db.instances.set(args, function(err) {
        expect(err).to.not.exist;

        db.results.getAll(args, function(err, params) {
          expect(err).to.not.exist;
          expect(params).to.include.keys('foo', 'fox');
          expect(params.foo).to.equal('bar');
          expect(params.fox).to.equal('bay');

          done();
        });
      });
    });
  });
});
