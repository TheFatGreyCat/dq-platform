import { describe, expect, it } from 'vitest';
import {
  canonicalDecimalOrNull,
  validateDraftSuite,
  validateDraftPolicy,
  isQuarantineEligible,
  createEmptyRuleDraft,
  createDefaultPolicyDraft,
  serializeSuitePayload,
  ROW_LEVEL_KINDS,
} from './revisionDraft';
import type { Suite } from '../../api/types';

describe('revisionDraft', () => {
  describe('canonicalDecimalOrNull', () => {
    it('validates canonical decimals and rejects exponents, leading zeroes, trailing zeroes', () => {
      expect(canonicalDecimalOrNull('0')).toBe('0');
      expect(canonicalDecimalOrNull('1')).toBe('1');
      expect(canonicalDecimalOrNull('0.5')).toBe('0.5');
      expect(canonicalDecimalOrNull('0.05')).toBe('0.05');

      expect(canonicalDecimalOrNull('1e3')).toBeNull();
      expect(canonicalDecimalOrNull('01')).toBeNull();
      expect(canonicalDecimalOrNull('1.50')).toBeNull();
      expect(canonicalDecimalOrNull('1.')).toBeNull();
      expect(canonicalDecimalOrNull('-0')).toBeNull();
      expect(canonicalDecimalOrNull('abc')).toBeNull();
    });
  });

  describe('isQuarantineEligible', () => {
    it('allows quarantine only for row-level kinds at CANDIDATE stage and PARTITION scope', () => {
      for (const kind of ROW_LEVEL_KINDS) {
        expect(isQuarantineEligible(kind, 'CANDIDATE', 'PARTITION')).toBe(true);
        expect(isQuarantineEligible(kind, 'RAW', 'PARTITION')).toBe(false);
        expect(isQuarantineEligible(kind, 'CANDIDATE', 'TABLE')).toBe(false);
      }
      expect(isQuarantineEligible('size', 'CANDIDATE', 'PARTITION')).toBe(false);
      expect(isQuarantineEligible('mean', 'CANDIDATE', 'PARTITION')).toBe(false);
      expect(isQuarantineEligible('schema_columns', 'CANDIDATE', 'PARTITION')).toBe(false);
    });
  });

  describe('validateDraftSuite', () => {
    it('detects duplicate rule IDs', () => {
      const r1 = createEmptyRuleDraft('r1', 'WRITE_GATE');
      const r2 = createEmptyRuleDraft('r1', 'WRITE_GATE');
      const errors = validateDraftSuite({
        workflow: 'WRITE_GATE',
        description: '',
        parameters: {},
        rules: [r1, r2],
      });
      expect(errors.some(e => e.field.includes('rule_id') && e.message.includes('trùng'))).toBe(true);
    });

    it('detects undeclared parameter references', () => {
      const r1 = createEmptyRuleDraft('r1', 'WRITE_GATE');
      r1.check = {
        kind: 'sql_predicate',
        predicate: 'col > ${undeclared_param}',
        columns: ['col'],
        assert: null,
      };
      const errors = validateDraftSuite({
        workflow: 'WRITE_GATE',
        description: '',
        parameters: { declared_param: '10' },
        rules: [r1],
      });
      expect(errors.some(e => e.message.includes('chưa khai báo'))).toBe(true);
    });

    it('detects between assertion min > max', () => {
      const r1 = createEmptyRuleDraft('r1', 'WRITE_GATE');
      r1.check = {
        kind: 'completeness',
        column: 'col',
        assert: {
          op: 'between',
          min: '0.9',
          max: '0.5',
        },
      };
      const errors = validateDraftSuite({
        workflow: 'WRITE_GATE',
        description: '',
        parameters: {},
        rules: [r1],
      });
      expect(errors.some(e => e.message.includes('min > max'))).toBe(true);
    });

    it('detects stage mismatch for workflow', () => {
      const r1 = createEmptyRuleDraft('r1', 'WRITE_GATE');
      r1.stage = 'AUDIT_SNAPSHOT';
      const errors = validateDraftSuite({
        workflow: 'WRITE_GATE',
        description: '',
        parameters: {},
        rules: [r1],
      });
      expect(errors.some(e => e.field.includes('stage'))).toBe(true);
    });
  });

  describe('serialize and parse roundtrip', () => {
    it('roundtrips valid suite with 9007199254740993 int64 value without precision loss', () => {
      const r1 = createEmptyRuleDraft('r1', 'WRITE_GATE');
      r1.check = {
        kind: 'size',
        assert: {
          op: '>=',
          value: '9007199254740993',
        },
      };
      const json = serializeSuitePayload({
        workflow: 'WRITE_GATE',
        description: 'Large int64 test',
        parameters: {},
        rules: [r1],
      });
      expect(json).toContain('9007199254740993');
      // Verify parsed payload retains exact string representation or big int
      const parsed: unknown = JSON.parse(json);
      expect(parsed).toBeTruthy();
      if (parsed && typeof parsed === 'object' && 'rules' in parsed && Array.isArray(parsed.rules)) {
        const firstRule = parsed.rules[0];
        if (firstRule && typeof firstRule === 'object' && 'check' in firstRule) {
          const checkObj = firstRule.check;
          if (checkObj && typeof checkObj === 'object' && 'assert' in checkObj) {
            const assertObj = checkObj.assert;
            if (assertObj && typeof assertObj === 'object' && 'value' in assertObj) {
              expect(String(assertObj.value)).toBe('9007199254740993');
            }
          }
        }
      }
    });
  });

  describe('validateDraftPolicy', () => {
    it('disallows QUARANTINE for non-eligible rule', () => {
      const suitePayload: Suite = {
        format: 'dq-rules/v3',
        workflow: 'WRITE_GATE',
        rules: [{
          rule_id: 'r1',
          dimension: 'validity',
          scope: 'TABLE',
          stage: 'CANDIDATE',
          check: { kind: 'complete', column: 'id' },
        }],
      };
      const policyDraft = createDefaultPolicyDraft(suitePayload.rules);
      const targetRule = policyDraft.rules[0];
      if (targetRule) {
        targetRule.action = 'QUARANTINE';
      }

      const errors = validateDraftPolicy(policyDraft, suitePayload.rules);
      expect(errors.some(e => e.message.includes('QUARANTINE'))).toBe(true);
    });
  });
});
